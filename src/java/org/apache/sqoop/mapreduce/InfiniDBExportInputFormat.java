/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.mapreduce.CombineFileInputFormat;
import org.apache.sqoop.mapreduce.CombineFileInputFormat.OneBlockInfo;
import org.apache.sqoop.mapreduce.CombineFileInputFormat.OneFileInfo;
import org.apache.sqoop.mapreduce.db.InfiniDBInputFormat.InfiniDBInputSplit;

/**
 * InputFormat that generates a user-defined number of splits to inject data
 * into the database.
 */
public class InfiniDBExportInputFormat
   extends ExportInputFormat {

  public static final Log LOG =
     LogFactory.getLog(InfiniDBExportInputFormat.class.getName());

  public InfiniDBExportInputFormat() {
  }

  
  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
    throws IOException {

	List<InfiniDBExportSplitData> PMSplits = new ArrayList<InfiniDBExportSplitData>();
    List<InputSplit> splits = new ArrayList<InputSplit>();
    
    // all blocks for all the files in input set
    OneFileInfo[] files;

    // mapping from a rack name to the set of Nodes in the rack
	HashMap<String, Set<String>> rackToNodes = new HashMap<String, Set<String>>();

    // mapping from a rack name to the list of blocks it has
    HashMap<String, List<OneBlockInfo>> rackToBlocks =
                              new HashMap<String, List<OneBlockInfo>>();

    // mapping from a block to the nodes on which it has replicas
    HashMap<OneBlockInfo, String[]> blockToNodes =
                              new HashMap<OneBlockInfo, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, List<OneBlockInfo>> nodeToBlocks =
                              new HashMap<String, List<OneBlockInfo>>();

	Configuration conf = jobContext.getConfiguration();

    // Get all the files in input set
    Path[] filePaths = FileUtil.stat2Paths(
                     listStatus(jobContext).toArray(new FileStatus[0]));
    if (filePaths.length == 0) {
      return splits;
    }
    files = new OneFileInfo[filePaths.length];

    // Populate all the blocks for all files
    // The OneFileInfo constructor fills out rackToBlocks, blockToNodes, 
    // nodeToBlocks and rackToNodes.
    for (int i = 0; i < filePaths.length; i++) {
      files[i] = new OneFileInfo(filePaths[i], conf, rackToBlocks, blockToNodes,
                                 nodeToBlocks, rackToNodes);
    }

    // Ask InfiniDB the names of all the PM hosts.
    String line;
    ProcessBuilder pb = new ProcessBuilder("/usr/local/Calpont/bin/calpontConsole", "getModuleHostNames", "pm");
    final Process process = pb.start();
    InputStream is = process.getInputStream();
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);

    // Skip the first line, as it's just stuff, not a hostname.
    line = br.readLine();
    // Create split data objects (we'll create the CombineFileSplits
    // from these). One split per PM.
    while ((line = br.readLine()) != null) {
      if (line.length() > 0)
      {
        PMSplits.add(new InfiniDBExportSplitData(line));
      }
    }
    
    // Look at all the blocks and assign to a split.
    // For each block, find the PM that contains the data. If more
    // than one is found, assign to the one with the least number of
    // blocks assigned./ If data is not on any PM, assign to a PM in
    // the same rack if possible. Otherwise assign to the PM with the
    // lowest number of blocks.
    InfiniDBExportSplitData currentSplit = null;
    for (OneFileInfo file : files) {
      OneBlockInfo[] blocks = file.getBlocks();
	
      // For each block, look at locations
      for (OneBlockInfo oneblock : blocks) {
    	for (String host : oneblock.hosts) {
    	  for (InfiniDBExportSplitData split : PMSplits) {
    		if (split.GetLocation().equals(host)) {
     		  if (currentSplit == null || 
    		      split.GetBlockCount() < currentSplit.GetBlockCount()) {
    			currentSplit = split;
    		  }
    		}
    	  }
        }
        if (currentSplit == null) {
    	  // If we get here, then the data exists on nodes that are not PMs
    	  // Look for the PM on the same rack with the lowest block count. 
    	  for (String rack : oneblock.racks) {
    		Set<String> rackHosts = rackToNodes.get(rack);
    		for (String host : rackHosts) {
    	      for (InfiniDBExportSplitData split : PMSplits) {
    	    	if (split.GetLocation().equals(host)) {
       		      if (currentSplit == null || 
      		          split.GetBlockCount() < currentSplit.GetBlockCount()) {
      			    currentSplit = split;
       		      }
    	    	}
      		  }
      		}
      	  }
    	}
      	if (currentSplit == null) {
      	  // If we get here, then the data exists on nodes that are not PMs
      	  // and on racks without PMs. Assign to the PM with the lowest block count
      	  for (InfiniDBExportSplitData split : PMSplits) {
       	    if (currentSplit == null || 
        	    split.GetBlockCount() < currentSplit.GetBlockCount()) {
        	  currentSplit = split;
        	}
      	  }
    	}
        if (currentSplit == null) {
          throw new IOException("While building splits, a block " + file.getFilePath().toString()
                  + " offset " + oneblock.offset + " length " + oneblock.length
                  + " was not assigned to a split");
        }
  	    // Add the block to the split
        currentSplit.AddBlock(oneblock);
      }
    }

    // create CombineFileSplit objects from PMSplits.
    // CombineFileSplits are what hadoop needs.
    for (InfiniDBExportSplitData split : PMSplits) {
      LOG.info("Adding blocks to split " + split.GetLocation());
      int size = (int)split.GetBlockCount();
      List<OneBlockInfo> blocks = split.GetBlocks();
      String [] locations = new String [1];
      locations[0] = split.GetLocation();
      Path[] paths = new Path[size];
      long[] offset = new long[size];
      long[] length = new long[size];
      for (int i = 0; i < blocks.size(); i++) {
        paths[i] = blocks.get(i).onepath;
        offset[i] = blocks.get(i).offset;
        length[i] = blocks.get(i).length;
        LOG.info("   path " + paths[i].toString() + " offset " + offset[i] + " length " + length[i]);
      }
      // add this split to the list that is returned
      splits.add(new CombineFileSplit(paths, offset, length, locations));
    }

    jobContext.getConfiguration().setInt(ConfigurationConstants.PROP_MAPRED_MAP_TASKS, splits.size());
    return splits;
  }

  private class InfiniDBExportSplitData {
    private String location;
    private List<OneBlockInfo> blocks = new ArrayList<OneBlockInfo>();
    
    public InfiniDBExportSplitData() {}
    public InfiniDBExportSplitData(String location) {
      this.location = location;
    }
    
    public void SetLocation(String location) {
      this.location = location;
    }
    
    public String GetLocation() {
      return this.location;
    }

    public void AddBlock(OneBlockInfo block) {
      blocks.add(block);	
    }
    
    public List<OneBlockInfo> GetBlocks() {
      return blocks;	
    }

    public long GetBlockCount() {
   	  return blocks.size();
    }
  }

}
