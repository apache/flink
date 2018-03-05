package org.apache.flink.addons.hbase;

import org.apache.hadoop.hbase.client.Scan;

/**
 * Interface shared by {@link TableInputFormat} and {@link TableSnapshotInputFormat},
 * subclass of the table input format should override methods defined in this interface.
 */
public interface HBaseTableScannerAware {

	/**
	 * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
	 *
	 * @return The appropriate instance of Scan for this usecase.
	 */
	Scan getScanner();

	/**
	 * What table is to be read.
	 * Per instance of a TableInputFormat derivative only a single tablename is possible.
	 *
	 * @return The name of the table
	 */
	String getTableName();
}
