package org.apache.flink.addons.hbase.util;

import org.apache.flink.addons.hbase.AbstractTableInputFormat;
import org.apache.flink.addons.hbase.TableInputSplit;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class for HBase connector module.
 */
public class HBaseConnectorUtil {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractTableInputFormat.class);

	/**
	 * Create an {@link HTable} instance and set it into this format.
	 */
	public static HTable createTable(String tableName, org.apache.hadoop.conf.Configuration hConf) {
		LOG.info("Initializing HBaseConfiguration");
		if (hConf == null) {
			//use files found in the classpath
			hConf = HBaseConfiguration.create();
		}

		try {
			return new HTable(hConf, tableName);
		} catch (Exception e) {
			LOG.error("Error instantiating a new HTable instance", e);
		}
		return null;
	}

	/**
	 * Log {@link TableInputSplit}.
	 *
	 * @param action action taken, for example open or created.
	 * @param split table input split.
	 * @param caller where the method is called, this can be class name
	 */
	public static void logSplitInfo(String action, TableInputSplit split, String caller) {
		int splitId = split.getSplitNumber();
		String splitStart = Bytes.toString(split.getStartRow());
		String splitEnd = Bytes.toString(split.getEndRow());
		String splitStartKey = splitStart.isEmpty() ? "-" : splitStart;
		String splitStopKey = splitEnd.isEmpty() ? "-" : splitEnd;
		String[] hostnames = split.getHostnames();
		LOG.info("{} split (this={})[{}|{}|{}|{}]", action, caller, splitId, hostnames, splitStartKey, splitStopKey);
	}
}
