package org.apache.sqoop.mapreduce;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.sqoop.manager.MySQLManager;
import org.apache.sqoop.util.AsyncSink;
import org.apache.sqoop.util.JdbcUrl;
import org.apache.sqoop.util.LoggingAsyncSink;
import org.apache.sqoop.util.NullAsyncSink;
import org.apache.sqoop.util.TaskId;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.manager.MySQLUtils;
import org.apache.sqoop.io.NamedFifo;

/**
 * Mapper that starts a 'cpimport' process and uses that to export rows from
 * HDFS to an InfiniDB database at high speed.
 * 
 * map() methods are actually provided by subclasses that read from
 * SequenceFiles (containing existing SqoopRecords) or text files (containing
 * delimited lines) and deliver these results to the fifo used to interface with
 * mysqlimport.
 */
public class InfiniDBExportMapper<KEYIN, VALIN> extends
		SqoopMapper<KEYIN, VALIN, NullWritable, NullWritable> {

	public static final Log LOG = LogFactory.getLog(InfiniDBExportMapper.class
			.getName());

	protected Configuration conf;

	/** The FIFO being used to communicate with cpimport. */
	protected File fifoFile;

	/** The process object representing the active connection to cpimport. */
	protected Process cpImportProcess;

	/** The stream to write to stdin for cpimport. */
	protected OutputStream importStream;

	// Handlers for stdout and stderr from cpimport.
	protected AsyncSink outSink;
	protected AsyncSink errSink;

	/** Character set used to write to cpimport. */
	/** todo- does cpimport support this */
	protected String cpCharSet;

	/**
	 * Create a named FIFO, and start cpimport connected to that FIFO. A File
	 * object representing the FIFO is in 'fifoFile'.
	 */
	private void initCpImportProcess() throws IOException {

		File taskAttemptDir = TaskId.getLocalWorkPath(conf);

		String tablename = conf.get(MySQLUtils.TABLE_NAME_KEY, "UNKNOWN_TABLE");
		this.fifoFile = new File(taskAttemptDir, tablename + ".txt");
		String filename = fifoFile.toString();

		// Create the FIFO itself.
		try {
			new NamedFifo(this.fifoFile).create();
		} catch (IOException ioe) {
			// Command failed.
			LOG.error("Could not mknod " + filename);
			this.fifoFile = null;
			throw new IOException(
					"Could not create FIFO to interface with cpimport", ioe);
		}

		// Now open the connection to cpimport.
		ArrayList<String> args = new ArrayList<String>();

		String connectString = conf.get(MySQLUtils.CONNECT_STRING_KEY);
		String databaseName = JdbcUrl.getDatabaseName(connectString);
		// skip these for now because we assume a local infinidb installation
		// but in the future can use these to start an ssh invocation of the
		// cpimport
		// String hostname = JdbcUrl.getHostName(connectString);
		// int port = JdbcUrl.getPort(connectString);

		if (null == databaseName) {
			throw new IOException("Could not determine database name");
		}

		String cpimport_path = conf.get(DBConfiguration.INFINIDB_BIN_PATH,
				DBConfiguration.DEFAULT_INFINIDB_BIN_PATH);
		LOG.info("cpimport_path=" + cpimport_path);
		args.add(cpimport_path + "/cpimport_sqoop"); // needs to be on the path.

		args.add(databaseName);
		args.add(tablename);
		args.add(filename);

		// Specify the delimiters to use.
		// InfiniDB cpimport default delimiter is '|'
		int fieldDelim = conf.getInt(MySQLUtils.INPUT_FIELD_DELIM_KEY, (int) '|');
		int enclosedBy = conf.getInt(MySQLUtils.INPUT_ENCLOSED_BY_KEY, 0);
		int escapedBy = conf.getInt(MySQLUtils.INPUT_ESCAPED_BY_KEY, 0);

		args.add("-s " + MySQLUtils.convertDelimToString(fieldDelim));
		if (0 != enclosedBy) {
			args.add("-E " + MySQLUtils.convertDelimToString(enclosedBy));
		}
		if (0 != escapedBy) {
			args.add("-C " + MySQLUtils.convertDelimToString(escapedBy));
		}

		// Add a rejection log directory. The default seems to go to a temp dir
		// that gets erased when the job ends. Not very useful.
		// Here we put it in the same directory as the bulk log.
		String errDir = conf.get(DBConfiguration.INFINIDB_BIN_PATH,
				DBConfiguration.DEFAULT_INFINIDB_BIN_PATH) + "/../data/bulk"; // default
		String line;
		String cc_path = conf.get(DBConfiguration.INFINIDB_BIN_PATH,
				DBConfiguration.DEFAULT_INFINIDB_BIN_PATH);
		ProcessBuilder pb = new ProcessBuilder(cc_path + "/getConfig",
				"WriteEngine", "BulkRoot");
		final Process process = pb.start();
		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);

		// There should be exactly one line, but we'll look for the first
		// non-empty line.
		while ((line = br.readLine()) != null) {
			if (line.length() > 0) {
				errDir = line;
				break;
			}
		}

		errDir = errDir + "/log/";
		args.add("-L " + errDir);

		// Begin the export in an external process.
		LOG.info("Starting cpimport with arguments:");
		for (String arg : args) {
			LOG.info("  " + arg);
		}

		// Actually start cpimport.
		cpImportProcess = Runtime.getRuntime()
				.exec(args.toArray(new String[0]));

		// Log everything it writes to stderr and stdout.
		this.outSink = new LoggingAsyncSink(LOG);
		this.outSink.processStream(cpImportProcess.getInputStream());

		this.errSink = new LoggingAsyncSink(LOG);
		this.errSink.processStream(cpImportProcess.getErrorStream());

		// Open the named FIFO after starting cpimport.
		this.importStream = new BufferedOutputStream(new FileOutputStream(
				fifoFile));

		// At this point, cpimport is running and hooked up to our FIFO.
		// The mapper just needs to populate it with data.
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		this.conf = context.getConfiguration();
		setup(context);
		initCpImportProcess();
		try {
			while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
			cleanup(context);
		} finally {
			// Shut down the cpimport process.
			closeExportHandles();
		}
	}

	private void closeExportHandles() throws IOException, InterruptedException {
		int ret = 0;
		if (null != this.importStream) {
			// Close the stream that writes to cpimport's stdin first.
			LOG.debug("Closing import stream");
			this.importStream.close();
			this.importStream = null;
		}

		if (null != this.cpImportProcess) {
			// We started cpimport; wait for it to finish.
			LOG.info("Waiting for cpimport to complete");
			ret = this.cpImportProcess.waitFor();
			LOG.info("cpimport closed connection");
			this.cpImportProcess = null;
		}

		// Finish processing any output from cpimport.
		// This is informational only, so we don't care about return codes.
		if (null != outSink) {
			LOG.debug("Waiting for any additional stdout from cpimport");
			outSink.join();
			outSink = null;
		}

		if (null != errSink) {
			LOG.debug("Waiting for any additional stderr from cpimport");
			errSink.join();
			errSink = null;
		}

		if (this.fifoFile != null && this.fifoFile.exists()) {
			// Clean up the resources we created.
			LOG.debug("Removing fifo file");
			if (!this.fifoFile.delete()) {
				LOG.error("Could not clean up named FIFO after completing mapper");
			}

			// We put the FIFO file in a one-off subdir. Remove that.
			File fifoParentDir = this.fifoFile.getParentFile();
			LOG.debug("Removing task attempt tmpdir");
			if (!fifoParentDir.delete()) {
				LOG.error("Could not clean up task dir after completing mapper");
			}

			this.fifoFile = null;
		}

		if (0 != ret) {
			// Don't mark the task as successful if cpimport returns an error.
			throw new IOException("cpimport terminated with error code " + ret);
		}
	}

	@Override
	protected void setup(Context context) {
		this.conf = context.getConfiguration();

		// TODO: Support additional encodings.
		// rtw-TODO: figure out if this is relevant
		this.cpCharSet = MySQLUtils.MYSQL_DEFAULT_CHARSET;

	}

	/**
	 * Takes a delimited text record (e.g., the output of a 'Text' object),
	 * re-encodes it for consumption by cpimport, and writes it to the pipe.
	 * 
	 * @param record
	 *            A delimited text representation of one record.
	 * @param terminator
	 *            an optional string that contains delimiters that terminate the
	 *            record (if not included in 'record' itself).
	 */
	protected void writeRecord(String record, String terminator)
			throws IOException, InterruptedException {

		// We've already set up cpimport to accept the same delimiters,
		// so we don't need to convert those. But our input text is UTF8
		// encoded; mysql allows configurable encoding, but defaults to
		// latin-1 (ISO8859_1). We'll convert to latin-1 for now.
		// TODO: Support user-configurable encodings.
		// rtw-TODO: see if this makes sense for InfiniDB

		byte[] mysqlBytes = record.getBytes(this.cpCharSet);
		this.importStream.write(mysqlBytes, 0, mysqlBytes.length);

		if (null != terminator) {
			byte[] termBytes = terminator.getBytes(this.cpCharSet);
			this.importStream.write(termBytes, 0, termBytes.length);
		}
	}
}
