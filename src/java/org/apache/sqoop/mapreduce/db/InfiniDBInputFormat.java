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
package org.apache.sqoop.mapreduce.db;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import org.apache.sqoop.mapreduce.db.InfiniDBRecordReader;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat.DBInputSplit;

import java.util.Collection;
import java.util.StringTokenizer;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


/**
 * A InputFormat that reads input data from an SQL table.
 * Operates like DBInputFormat, but instead of using LIMIT and OFFSET to
 * demarcate splits, it tries to generate WHERE clauses which separate the
 * data into roughly equivalent shards.
 */
public class InfiniDBInputFormat<T extends DBWritable>
      extends DataDrivenDBInputFormat<T> implements Configurable  {

  private static final Log LOG =
      LogFactory.getLog(InfiniDBInputFormat.class);

  @Override
  /** {@inheritDoc} */
  public RecordReader<LongWritable, T> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    Class<T> inputClass = (Class<T>) (getDBConf().getInputClass());
    try {
      return new InfiniDBRecordReader<T>((InfiniDBInputSplit)split, inputClass,
	  	context.getConfiguration(), getConnection(), getDBConf());
    } catch (SQLException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  /** {@inheritDoc} */
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
	List<InputSplit> splits = new ArrayList<InputSplit>();

	Configuration conf = jobContext.getConfiguration();

	// Ask InfiniDB the names of all the hosts.
    String line;
    String cc_path = conf.get(DBConfiguration.INFINIDB_BIN_PATH, DBConfiguration.DEFAULT_INFINIDB_BIN_PATH);
    ProcessBuilder pb = new ProcessBuilder(cc_path + "/calpontConsole", "getModuleHostNames", "pm");
    final Process process = pb.start();
    InputStream is = process.getInputStream();
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);

    // generate splits
    
    // Skip non hostname lines
    while ((line = br.readLine()) != null) {
    	// When we find the word "getmodulehostnames", we know the
    	// actual hostnames are next. All the stuff preceding is junk.
        if (line.contains("getmodulehostnames"))
        	break;
    }    
    // One hostname per line.
    while ((line = br.readLine()) != null) {
      if (line.length() > 0)
      {
        LOG.info("Adding " + line + " to splits");
        boolean global = conf.getBoolean(org.apache.sqoop.config.ConfigurationHelper.getInfiniDBGlobalProperty(), false);
        splits.add(new InfiniDBInputSplit(line, conf.getBoolean(org.apache.sqoop.config.ConfigurationHelper.getInfiniDBGlobalProperty(), false)));
      }
    }
    
    conf.setInt(ConfigurationConstants.PROP_MAPRED_MAP_TASKS, splits.size());
    return splits;
  }

  /**
   * Returns a list of all the server hostnames that hdfs is aware of.
   * We don't currently use this, but it's such a nice little tool that
   * I'm leaving it here. If, for example, we were sure that every hdfs 
   * node was an InfiniDB PM, then this would most likely be superior to
   * the way we get the ip's for our splits.
   */
  private String[] getActiveServersList(JobContext context){
      String [] servers = null;
       try {
                JobClient jc = new JobClient((JobConf)context.getConfiguration());
                ClusterStatus status = jc.getClusterStatus(true);
                Collection<String> atc = status.getActiveTrackerNames();
                servers = new String[atc.size()];
                int s = 0;
                for(String serverInfo : atc){
                         StringTokenizer st = new StringTokenizer(serverInfo, ":");
                         String trackerName = st.nextToken();
                         StringTokenizer st1 = new StringTokenizer(trackerName, "_");
                         st1.nextToken();
                         servers[s++] = st1.nextToken();
                }
      }catch (IOException e) {
                e.printStackTrace();
      }

      return servers;
  }

  // Configuration methods override superclass to ensure that the proper
  // DataDrivenDBInputFormat gets used.

  /** Note that the "orderBy" column is called the "splitBy" in this version.
    * We reuse the same field, but it's not strictly ordering it
    * -- just partitioning the results.
    */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String tableName, String conditions,
      String splitBy, String... fieldNames) {
    DBInputFormat.setInput(job, inputClass, tableName, conditions,
        splitBy, fieldNames);
    job.setInputFormatClass(InfiniDBInputFormat.class);
  }

  /** setInput() takes a custom query and a separate "bounding query" to use
      instead of the custom "count query". We ifnore the bounding query.
    */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String inputQuery, String inputBoundingQuery) {
    DBInputFormat.setInput(job, inputClass, inputQuery, "");
    job.setInputFormatClass(InfiniDBInputFormat.class);
  }

  /**
   * A InputSplit that represents a single InfiniDB Performance Module.
   */
  public static class InfiniDBInputSplit
      extends com.cloudera.sqoop.mapreduce.db.DBInputFormat.DBInputSplit {

    private static final Log LOG =
	      LogFactory.getLog(InfiniDBInputSplit.class);
	private String[] hostNames;
	private boolean infinidbGlobal;
	/**
     * Default Constructor.
     */
    public InfiniDBInputSplit() {
        hostNames = new String[1];
    }

    /**
     * Convenience Constructor.
     * @param hostName the name to return in getLocation()
     */
    public InfiniDBInputSplit(final String hostName, final boolean infinidbGlobal) {
      hostNames = new String[1];
      this.hostNames[0] = hostName;
      this.infinidbGlobal = infinidbGlobal;
    }

    /**
     * @return The total row count in this split.
     */
    @Override
    public long getLength() throws IOException {
      return 0; // unfortunately, we don't know this.
    }

    /**
     * @return an array of length one containing the 
     * hostname of the PM
     */
    @Override
    public String[] getLocations() throws IOException {
       return hostNames;
    }

    @Override
    /** {@inheritDoc} */
    public void readFields(DataInput input) throws IOException {
      this.hostNames[0] = Text.readString(input);
      this.infinidbGlobal = input.readBoolean();
    }

    @Override
    /** {@inheritDoc} */
    public void write(DataOutput output) throws IOException {
      Text.writeString(output, this.hostNames[0]);
      output.writeBoolean(this.infinidbGlobal);
    }

    public String getHostName() {
      return hostNames[0];
    }
    
    public boolean getInfinidbGlobal() {
      return infinidbGlobal;
    }
    
    public void setInfiniGlobal(boolean infinidbGlobal) {
      this.infinidbGlobal = infinidbGlobal;
    }
  }
}
