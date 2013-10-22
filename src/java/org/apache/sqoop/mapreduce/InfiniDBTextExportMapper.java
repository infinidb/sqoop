package org.apache.sqoop.mapreduce;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.cloudera.sqoop.manager.MySQLUtils;
import org.apache.sqoop.mapreduce.InfiniDBExportMapper;

/**
 * cpimport-based exporter which accepts lines of text from files
 * in HDFS to emit to the database.
 */
public class InfiniDBTextExportMapper
    extends InfiniDBExportMapper<LongWritable, Text> {

  // End-of-record delimiter.
  private String recordEndStr;

  @Override
  protected void setup(Context context) {
    super.setup(context);

    char recordDelim = (char) conf.getInt(MySQLUtils.OUTPUT_RECORD_DELIM_KEY,
        (int) '\n');
    this.recordEndStr = "" + recordDelim;
  }

  /**
   * Export the table to InfiniDB by using cpimporty to write the data to the
   * database.
   *
   * Expects one delimited text record as the 'val'; ignores the key.
   */
  @Override
  public void map(LongWritable key, Text val, Context context)
      throws IOException, InterruptedException {

    writeRecord(val.toString(), this.recordEndStr);

    // We don't emit anything to the OutputCollector because we wrote
    // straight to InfiniDB. Send a progress indicator to prevent a timeout.
    context.progress();
  }

}
