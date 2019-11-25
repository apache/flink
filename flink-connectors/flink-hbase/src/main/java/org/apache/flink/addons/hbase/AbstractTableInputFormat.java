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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

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
 * Abstract {@link InputFormat} to read data from HBase tables.
 */
public abstract class AbstractTableInputFormat<T> extends RichInputFormat<T, TableInputSplit> {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractTableInputFormat.class);

	// helper variable to decide whether the input is exhausted or not
	protected boolean endReached = false;

	protected transient HTable table = null;
	protected transient Scan scan = null;

	/** HBase iterator wrapper. */
	protected ResultScanner resultScanner = null;

	protected byte[] currentRow;
	protected long scannedRows;

	/**
	 * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
	 *
	 * @return The appropriate instance of Scan for this use case.
	 */
	protected abstract Scan getScanner();

	/**
	 * What table is to be read.
	 *
	 * <p>Per instance of a TableInputFormat derivative only a single table name is possible.
	 *
	 * @return The name of the table
	 */
	protected abstract String getTableName();

	/**
	 * HBase returns an instance of {@link Result}.
	 *
	 * <p>This method maps the returned {@link Result} instance into the output type {@link T}.
	 *
	 * @param r The Result instance from HBase that needs to be converted
	 * @return The appropriate instance of {@link T} that contains the data of Result.
	 */
	protected abstract T mapResultToOutType(Result r);

	/**
	 * Creates a {@link Scan} object and opens the {@link HTable} connection.
	 *
	 * <p>These are opened here because they are needed in the createInputSplits
	 * which is called before the openInputFormat method.
	 *
	 * <p>The connection is opened in this method and closed in {@link #closeInputFormat()}.
	 *
	 * @param parameters The configuration that is to be used
	 * @see Configuration
	 */
	public abstract void configure(Configuration parameters);

	@Override
	public void open(TableInputSplit split) throws IOException {
		if (table == null) {
			throw new IOException("The HBase table has not been opened! " +
				"This needs to be done in configure().");
		}
		if (scan == null) {
			throw new IOException("Scan has not been initialized! " +
				"This needs to be done in configure().");
		}
		if (split == null) {
			throw new IOException("Input split is null!");
		}

		logSplitInfo("opening", split);

		// set scan range
		currentRow = split.getStartRow();
		scan.setStartRow(currentRow);
		scan.setStopRow(split.getEndRow());

		resultScanner = table.getScanner(scan);
		endReached = false;
		scannedRows = 0;
	}

	public T nextRecord(T reuse) throws IOException {
		if (resultScanner == null) {
			throw new IOException("No table result scanner provided!");
		}
		Result res;
		try {
			res = resultScanner.next();
		} catch (Exception e) {
			resultScanner.close();
			//workaround for timeout on scan
			LOG.warn("Error after scan of " + scannedRows + " rows. Retry with a new scanner...", e);
			scan.withStartRow(currentRow, false);
			resultScanner = table.getScanner(scan);
			res = resultScanner.next();
		}

		if (res != null) {
			scannedRows++;
			currentRow = res.getRow();
			return mapResultToOutType(res);
		}

		endReached = true;
		return null;
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

	@Override
	public boolean reachedEnd() throws IOException {
		return endReached;
	}

	@Override
	public void close() throws IOException {
		LOG.info("Closing split (scanned {} rows)", scannedRows);
		currentRow = null;
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
			throw new IOException("The HBase table has not been opened! " +
				"This needs to be done in configure().");
		}
		if (scan == null) {
			throw new IOException("Scan has not been initialized! " +
				"This needs to be done in configure().");
		}

		// Get the starting and ending row keys for every region in the currently open table
		final Pair<byte[][], byte[][]> keys = table.getRegionLocator().getStartEndKeys();
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
			final String regionLocation = table.getRegionLocator().getRegionLocation(startKey, false).getHostnamePort();
			// Test if the given region is to be included in the InputSplit while splitting the regions of a table
			if (!includeRegionInScan(startKey, endKey)) {
				continue;
			}
			// Find the region on which the given row is being served
			final String[] hosts = new String[]{regionLocation};

			// Determine if regions contains keys used by the scan
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
		return splits.toArray(new TableInputSplit[splits.size()]);
	}

	/**
	 * Test if the given region is to be included in the scan while splitting the regions of a table.
	 *
	 * @param startKey Start key of the region
	 * @param endKey   End key of the region
	 * @return true, if this region needs to be included as part of the input (default).
	 */
	protected boolean includeRegionInScan(final byte[] startKey, final byte[] endKey) {
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
