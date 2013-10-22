package org.apache.sqoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.mapreduce.ExportJobBase;

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

}
