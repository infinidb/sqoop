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

package org.apache.sqoop.manager;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.mapreduce.db.InfiniDBInputFormat;
import org.apache.sqoop.mapreduce.InfiniDBImportJob;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hbase.HBaseUtil;
import com.cloudera.sqoop.mapreduce.DataDrivenImportJob;
import com.cloudera.sqoop.mapreduce.HBaseImportJob;
import com.cloudera.sqoop.mapreduce.ImportJobBase;
import com.cloudera.sqoop.util.ImportException;
/**
 * Manages connections to MySQL databases.
 */
public class InfiniDBManager extends org.apache.sqoop.manager.MySQLManager {

	public static final Log LOG = LogFactory.getLog(InfiniDBManager.class
			.getName());

	// driver class to ensure is loaded when making db connection.
	private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

	public InfiniDBManager(final SqoopOptions opts) {
		super(opts);
		markWarningPrinted(); // don't display the Mysql direct warning.
	}

	@Override
	public String getPrimaryKey(String tableName) {
		return "";
	}

	@Override
	public void importTable(com.cloudera.sqoop.manager.ImportJobContext context)
	    throws IOException, ImportException {
	
      context.setInputFormat(InfiniDBInputFormat.class);
      String tableName = context.getTableName();
      String jarFile = context.getJarFile();
      SqoopOptions opts = context.getOptions();

      context.setConnManager(this);

      ImportJobBase importer;
      if (opts.getHBaseTable() != null) {
        // Import to HBase.
        if (!HBaseUtil.isHBaseJarPresent()) {
          throw new ImportException("HBase jars are not present in "
              + "classpath, cannot import to HBase!");
        }
        // TODO: create InfiniDBHBaseImportJob
        throw new ImportException("HBase is not yet supported for InfiniDB import");
//        importer = new HBaseImportJob(opts, context);
      } else {
        // Import to HDFS.
        importer = new InfiniDBImportJob(opts, context.getInputFormat(),
		        context);
      }
      checkTableImportOptions(context);

      String splitCol = getSplitColumn(opts, tableName);
      importer.runImport(tableName, jarFile, splitCol, opts.getConf());
	}
	
	@Override
	public void importQuery(com.cloudera.sqoop.manager.ImportJobContext context)
	    throws IOException, ImportException {
      context.setInputFormat(InfiniDBInputFormat.class);
	  String jarFile = context.getJarFile();
	  SqoopOptions opts = context.getOptions();
	
	  context.setConnManager(this);
	
	  ImportJobBase importer;
	  if (opts.getHBaseTable() != null) {
        // Import to HBase.
	    if (!HBaseUtil.isHBaseJarPresent()) {
	      throw new ImportException("HBase jars are not present in classpath,"
	         + " cannot import to HBase!");
	    }
        // TODO: create InfiniDBHBaseImportJob
        throw new ImportException("HBase is not yet supported for InfiniDB import");
//	    importer = new HBaseImportJob(opts, context);
	  } else {
	    // Import to HDFS.
	    importer = new InfiniDBImportJob(opts, context.getInputFormat(),
	        context);
 	  }
		
      String splitCol = "";
      importer.runImport(null, jarFile, splitCol, opts.getConf());
    }

	@Override
	protected String getPrimaryKeyQuery(String tableName) {
		return "";
	}

	@Override
	public Map<String, Integer> getColumnTypes(String tableName, String sqlQuery)
			throws IOException {
		Map<String, Integer> columnTypes;
		if (null != tableName) {
			// We're generating a class based on a table import.
			columnTypes = getColumnTypes(tableName);
		} else {
			// This is based on an arbitrary query.
			columnTypes = getColumnTypesForQuery(sqlQuery);
		}
		return columnTypes;
	}

	public Map<String, String> getColumnTypeNames(String tableName,
			String sqlQuery) {
		Map<String, String> columnTypeNames;
		if (null != tableName) {
			// We're generating a class based on a table import.
			columnTypeNames = getColumnTypeNamesForTable(tableName);
		} else {
			// This is based on an arbitrary query.
			columnTypeNames = getColumnTypeNamesForQuery(sqlQuery);
		}
		return columnTypeNames;
	}

	@Override
	protected void checkTableImportOptions(
			com.cloudera.sqoop.manager.ImportJobContext context)
			throws IOException, ImportException {
	}

	@Override
	protected String getSplitColumn(SqoopOptions opts, String tableName) {
		return "";
	}
}

