package org.apache.sqoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.sqoop.mapreduce.InfiniDBExportInputFormat;
import org.apache.sqoop.manager.MySQLUtils;

import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.mapreduce.ExportJobBase;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;

public class InfiniDBExportJob extends MySQLExportJob {

	public static final Log LOG =
			LogFactory.getLog(InfiniDBExportJob.class.getName());
	
	public InfiniDBExportJob(final ExportJobContext context) {
		super(context);
	}
	
	@Override
	protected Class<? extends Mapper> getMapperClass() {
		if (inputIsSequenceFiles()) {
			return InfiniDBRecordExportMapper.class;
		} else {
			return InfiniDBTextExportMapper.class;
	    }
	}

	@Override
	protected Class<? extends InputFormat> getInputFormatClass()
      throws ClassNotFoundException {
      return InfiniDBExportInputFormat.class;
    }

	@Override
	/**
	 * Configure the inputformat to use for the job.
	 */
	protected void configureInputFormat(Job job, String tableName,
	    String tableClassName, String splitByCol)
	    throws ClassNotFoundException, IOException {
	
	  // Configure the delimiters, etc.
	  Configuration conf = job.getConfiguration();
	  conf.setInt(MySQLUtils.INPUT_FIELD_DELIM_KEY,
	      options.getInputFieldDelim());
	  conf.setInt(MySQLUtils.INPUT_RECORD_DELIM_KEY,
	      options.getInputRecordDelim());
	  conf.setInt(MySQLUtils.INPUT_ENCLOSED_BY_KEY,
	      options.getInputEnclosedBy());
	  conf.setInt(MySQLUtils.INPUT_ESCAPED_BY_KEY,
	      options.getInputEscapedBy());
	  conf.setBoolean(MySQLUtils.INPUT_ENCLOSE_REQUIRED_KEY,
	      options.isInputEncloseRequired());
	  String [] extraArgs = options.getExtraArgs();
	  if (null != extraArgs) {
	    conf.setStrings(MySQLUtils.EXTRA_ARGS_KEY, extraArgs);
	  }
	
	  ConnManager mgr = context.getConnManager();
	  String username = options.getUsername();
	  if (null == username || username.length() == 0) {
	    DBConfiguration.configureDB(job.getConfiguration(),
	        mgr.getDriverClass(), options.getConnectString());
	  } else {
	    DBConfiguration.configureDB(job.getConfiguration(),
	        mgr.getDriverClass(), options.getConnectString(), username,
	        options.getPassword());
	  }
	
	  String [] colNames = options.getColumns();
	  if (null == colNames) {
	    colNames = mgr.getColumnNames(tableName);
	  }
	
	  String [] sqlColNames = null;
	  if (null != colNames) {
	    sqlColNames = new String[colNames.length];
	    for (int i = 0; i < colNames.length; i++) {
	      sqlColNames[i] = mgr.escapeColName(colNames[i]);
	    }
	  }
	
	  // Note that mysqldump also does *not* want a quoted table name.
	  DataDrivenDBInputFormat.setInput(job, DBWritable.class,
	      tableName, null, null, sqlColNames);
	
	  // Configure the actual InputFormat to use.
	  super.configureInputFormat(job, tableName, tableClassName, splitByCol);
	}

}
