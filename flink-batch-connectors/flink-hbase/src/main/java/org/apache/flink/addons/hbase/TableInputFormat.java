/*
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
package org.apache.flink.addons.hbase;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link InputFormat} subclass that wraps the access for HTables.
 */
public abstract class TableInputFormat<T extends Tuple> extends RichInputFormat<T, TableInputSplit> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(TableInputFormat.class);

	/** helper variable to decide whether the input is exhausted or not */
	private boolean endReached = false;

	protected transient HTable table = null;
	protected transient Scan scan = null;

	/** HBase iterator wrapper */
	private ResultScanner resultScanner = null;

	private byte[] lastRow;
	private int scannedRows;

	/**
	 * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
	 * @return The appropriate instance of Scan for this usecase.
	 */
	protected abstract Scan getScanner();

	/**
	 * What table is to be read.
	 * Per instance of a TableInputFormat derivative only a single tablename is possible.
	 * @return The name of the table
	 */
	protected abstract String getTableName();

	/**
	 * The output from HBase is always an instance of {@link Result}.
	 * This method is to copy the data in the Result instance into the required {@link Tuple}
	 * @param r The Result instance from HBase that needs to be converted
	 * @return The approriate instance of {@link Tuple} that contains the needed information.
	 */
	protected abstract T mapResultToTuple(Result r);

	/**
	 * Creates a {@link Scan} object and opens the {@link HTable} connection.
	 * These are opened here because they are needed in the createInputSplits
	 * which is called before the openInputFormat method.
	 * So the connection is opened in {@link #configure(Configuration)} and closed in {@link #closeInputFormat()}.
	 *
	 * @param parameters The configuration that is to be used
	 * @see Configuration
	 */
	@Override
	public void configure(Configuration parameters) {
		table = createTable();
		if (table != null) {
			scan = getScanner();
		}
	}

	/**
	 * Create an {@link HTable} instance and set it into this format
	 */
	private HTable createTable() {
		LOG.info("Initializing HBaseConfiguration");
		//use files found in the classpath
		org.apache.hadoop.conf.Configuration hConf = HBaseConfiguration.create();

		try {
			return new HTable(hConf, getTableName());
		} catch (Exception e) {
			LOG.error("Error instantiating a new HTable instance", e);
		}
		return null;
	}

	@Override
	public void open(TableInputSplit split) throws IOException {
		if (table == null) {
			throw new IOException("The HBase table has not been opened!");
		}
		if (scan == null) {
			throw new IOException("getScanner returned null");
		}
		if (split == null) {
			throw new IOException("Input split is null!");
		}

		logSplitInfo("opening", split);
		scan.setStartRow(split.getStartRow());
		lastRow = split.getEndRow();
		scan.setStopRow(lastRow);

		resultScanner = table.getScanner(scan);
		endReached = false;
		scannedRows = 0;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return endReached;
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		if (resultScanner == null) {
			throw new IOException("No table result scanner provided!");
		}
		try {
			Result res = resultScanner.next();
			if (res != null) {
				scannedRows++;
				lastRow = res.getRow();
				return mapResultToTuple(res);
			}
		} catch (Exception e) {
			resultScanner.close();
			//workaround for timeout on scan
			LOG.warn("Error after scan of " + scannedRows + " rows. Retry with a new scanner...", e);
			scan.setStartRow(lastRow);
			resultScanner = table.getScanner(scan);
			Result res = resultScanner.next();
			if (res != null) {
				scannedRows++;
				lastRow = res.getRow();
				return mapResultToTuple(res);
			}
		}

		endReached = true;
		return null;
	}

	@Override
	public void close() throws IOException {
		LOG.info("Closing split (scanned {} rows)", scannedRows);
		lastRow = null;
		try {
			if (resultScanner != null) {
				resultScanner.close();
			}
		} finally {
			resultScanner = null;
		}
	}

	@Override
	public void closeInputFormat() throws IOException {
		try {
			if (table != null) {
				table.close();
			}
		} finally {
			table = null;
		}
	}

	@Override
	public TableInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
		if (table == null) {
			throw new IOException("The HBase table has not been opened!");
		}
		if (scan == null) {
			throw new IOException("getScanner returned null");
		}

		//Gets the starting and ending row keys for every region in the currently open table
		final Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
			throw new IOException("Expecting at least one region.");
		}
		final byte[] startRow = scan.getStartRow();
		final byte[] stopRow = scan.getStopRow();
		final boolean scanWithNoLowerBound = startRow.length == 0;
		final boolean scanWithNoUpperBound = stopRow.length == 0;

		final List<TableInputSplit> splits = new ArrayList<TableInputSplit>(minNumSplits);
		for (int i = 0; i < keys.getFirst().length; i++) {
			final byte[] startKey = keys.getFirst()[i];
			final byte[] endKey = keys.getSecond()[i];
			final String regionLocation = table.getRegionLocation(startKey, false).getHostnamePort();
			//Test if the given region is to be included in the InputSplit while splitting the regions of a table
			if (!includeRegionInSplit(startKey, endKey)) {
				continue;
			}
			//Finds the region on which the given row is being served
			final String[] hosts = new String[]{regionLocation};

			// determine if regions contains keys used by the scan
			boolean isLastRegion = endKey.length == 0;
			if ((scanWithNoLowerBound || isLastRegion || Bytes.compareTo(startRow, endKey) < 0) &&
				(scanWithNoUpperBound || Bytes.compareTo(stopRow, startKey) > 0)) {

				final byte[] splitStart = scanWithNoLowerBound || Bytes.compareTo(startKey, startRow) >= 0 ? startKey : startRow;
				final byte[] splitStop = (scanWithNoUpperBound || Bytes.compareTo(endKey, stopRow) <= 0)
					&& !isLastRegion ? endKey : stopRow;
				int id = splits.size();
				final TableInputSplit split = new TableInputSplit(id, hosts, table.getTableName(), splitStart, splitStop);
				splits.add(split);
			}
		}
		LOG.info("Created " + splits.size() + " splits");
		for (TableInputSplit split : splits) {
			logSplitInfo("created", split);
		}
		return splits.toArray(new TableInputSplit[0]);
	}

	private void logSplitInfo(String action, TableInputSplit split) {
		int splitId = split.getSplitNumber();
		String splitStart = Bytes.toString(split.getStartRow());
		String splitEnd = Bytes.toString(split.getEndRow());
		String splitStartKey = splitStart.isEmpty() ? "-" : splitStart;
		String splitStopKey = splitEnd.isEmpty() ? "-" : splitEnd;
		String[] hostnames = split.getHostnames();
		LOG.info("{} split (this={})[{}|{}|{}|{}]", action, this, splitId, hostnames, splitStartKey, splitStopKey);
	}

	/**
	 * Test if the given region is to be included in the InputSplit while splitting the regions of a table.
	 * <p>
	 * This optimization is effective when there is a specific reasoning to exclude an entire region from the M-R job,
	 * (and hence, not contributing to the InputSplit), given the start and end keys of the same. <br>
	 * Useful when we need to remember the last-processed top record and revisit the [last, current) interval for M-R
	 * processing, continuously. In addition to reducing InputSplits, reduces the load on the region server as well, due
	 * to the ordering of the keys. <br>
	 * <br>
	 * Note: It is possible that <code>endKey.length() == 0 </code> , for the last (recent) region. <br>
	 * Override this method, if you want to bulk exclude regions altogether from M-R. By default, no region is excluded(
	 * i.e. all regions are included).
	 *
	 * @param startKey Start key of the region
	 * @param endKey   End key of the region
	 * @return true, if this region needs to be included as part of the input (default).
	 */
	protected boolean includeRegionInSplit(final byte[] startKey, final byte[] endKey) {
		return true;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(TableInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return null;
	}

}
