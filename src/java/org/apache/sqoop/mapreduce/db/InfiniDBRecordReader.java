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


import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.sqoop.mapreduce.db.InfiniDBInputFormat.InfiniDBInputSplit;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat;
import com.cloudera.sqoop.mapreduce.db.DBRecordReader;
import com.cloudera.sqoop.util.LoggingUtils;

/**
 * A RecordReader that reads records from a SQL table,
 * using data-driven WHERE clause splits.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
public class InfiniDBRecordReader<T extends DBWritable>
    extends DBRecordReader<T> {

  private static final Log LOG =
      LogFactory.getLog(InfiniDBRecordReader.class);

  // CHECKSTYLE:OFF
  // TODO(aaron): Refactor constructor to use fewer arguments.
  /**
   * @param split The InputSplit to read data for
   * @throws SQLException
   */
  public InfiniDBRecordReader(InfiniDBInputFormat.InfiniDBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn,
      DBConfiguration dbConfig) throws SQLException {
    super((DBInputFormat.DBInputSplit)split, inputClass, conf, conn, dbConfig, dbConfig.getInputConditions(),
    		dbConfig.getInputFieldNames(), dbConfig.getInputTableName());
  }

  // CHECKSTYLE:ON
  @Override
  /** {@inheritDoc} */
  public float getProgress() throws IOException {
    return isDone() ? 1.0f : 0.0f;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) 
      throws IOException {
	System.out.println("Initializing InfiniDBRecordReader for " + ((InfiniDBInputSplit)split).getHostName());
    LOG.info("Initializing InfiniDBRecordReader for " + ((InfiniDBInputSplit)split).getHostName());
    // A good place to ensure this InfiniDB session has
	// infinidb_local_query = true
	try {
      Statement st = getConnection().createStatement();
      ResultSet res = st.executeQuery("SET @@infinidb_local_query=1");
    }
    catch (Exception ex) {
      System.out.println("Failed to set infinidb_local_query: " + StringUtils.stringifyException(ex));
      LOG.error("Failed to set infinidb_local_query: " + StringUtils.stringifyException(ex));
      throw new IOException("Failed to set infinidb_local_query");
    }
  }

  @Override
  /** Returns the query for selecting the records,
   * subclasses can override this for custom behaviour.*/
  protected String getSelectQuery() {
    StringBuilder query = new StringBuilder();

    // Default codepath for MySQL, HSQLDB, etc.
    // Relies on LIMIT/OFFSET for splits.
    if (dbConf.getInputQuery() == null) {
      query.append("SELECT ");

      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length -1) {
          query.append(", ");
        }
      }

      query.append(" FROM ").append(tableName);
      if (conditions != null && conditions.length() > 0) {
        query.append(" WHERE (").append(conditions).append(")");
      }

      String orderBy = dbConf.getInputOrderBy();
      if (orderBy != null && orderBy.length() > 0) {
        query.append(" ORDER BY ").append(orderBy);
      }
    } else {
      //PREBUILT QUERY
      query.append(dbConf.getInputQuery());
    }

    return query.toString();
  }
  
  @Override
  /** {@inheritDoc} */
  public boolean nextKeyValue() throws IOException {
    try {
      if (key == null) {
        key = new LongWritable();
      }
      if (value == null) {
        value = createValue();
      }
      if (null == this.results) {
        // First time into this method, run the query.
        this.results = executeQuery(getSelectQuery());
      }
      if (!results.next()) {
        return false;
      }

      // Set the key field value as the output key value
      key.set(1);

      value.readFields(results);
//  	System.out.println("InfiniDBRecordReader.nextKeyValue " + value.toString());

      pos++;
    } catch (SQLException e) {
      LoggingUtils.logAll(LOG, e);
      if (this.statement != null) {
        try {
          statement.close();
        } catch (SQLException ex) {
          LOG.error("Failed to close statement", ex);
        }
      }
      if (this.connection != null) {
        try {
          connection.close();
        } catch (SQLException ex) {
          LOG.error("Failed to close connection", ex);
        }
      }

      throw new IOException("SQLException in nextKeyValue", e);
    }
    return true;
  }
}
