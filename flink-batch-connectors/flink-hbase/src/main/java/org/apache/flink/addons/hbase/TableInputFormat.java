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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
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

/**
 * {@link InputFormat} subclass that wraps the access for HTables.
 *
 */
public abstract class TableInputFormat<T extends Tuple> extends RichInputFormat<T, TableInputSplit>{

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(TableInputFormat.class);

	/** helper variable to decide whether the input is exhausted or not */
	private boolean endReached = false;

	// TODO table and scan could be serialized when kryo serializer will be the default
	protected transient HTable table;
	protected transient Scan scan;

	/** HBase iterator wrapper */
	private ResultScanner rs;

	private byte[] lastRow;
	private int scannedRows;

	// abstract methods allow for multiple table and scanners in the same job
	protected abstract Scan getScanner();
	protected abstract String getTableName();
	protected abstract T mapResultToTuple(Result r);

	/**
	 * creates a {@link Scan} object and a {@link HTable} connection
	 *
	 * @param parameters
	 * @see Configuration
	 */
	@Override
	public void configure(Configuration parameters) {
		this.table = createTable();
		this.scan = getScanner();
	}

	/** Create an {@link HTable} instance and set it into this format */
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
	public boolean reachedEnd() throws IOException {
		return this.endReached;
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		if (this.rs == null){
			throw new IOException("No table result scanner provided!");
		}
		try{
			Result res = this.rs.next();
			if (res != null){
				scannedRows++;
				lastRow = res.getRow();
				return mapResultToTuple(res);
			}
		}catch (Exception e) {
			this.rs.close();
			//workaround for timeout on scan
			StringBuffer logMsg = new StringBuffer("Error after scan of ")
					.append(scannedRows)
					.append(" rows. Retry with a new scanner...");
			LOG.warn(logMsg.toString(), e);
			this.scan.setStartRow(lastRow);
			this.rs = table.getScanner(scan);
			Result res = this.rs.next();
			if (res != null) {
				scannedRows++;
				lastRow = res.getRow();
				return mapResultToTuple(res);
			}
		}

		this.endReached = true;
		return null;
	}

	@Override
	public void open(TableInputSplit split) throws IOException {
		if (split == null){
			throw new IOException("Input split is null!");
		}
		if (table == null){
			throw new IOException("No HTable provided!");
		}
		if (scan == null){
			throw new IOException("No Scan instance provided");
		}

		logSplitInfo("opening", split);
		scan.setStartRow(split.getStartRow());
		lastRow = split.getEndRow();
		scan.setStopRow(lastRow);

		this.rs = table.getScanner(scan);
		this.endReached = false;
		this.scannedRows = 0;
	}

	@Override
	public void close() throws IOException {
		if(rs!=null){
			this.rs.close();
		}
		if(table!=null){
			this.table.close();
		}
		LOG.info("Closing split (scanned {} rows)", scannedRows);
		this.lastRow = null;
	}

	@Override
	public TableInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
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
			final String[] hosts = new String[] { regionLocation };

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
		LOG.info("{} split [{}|{}|{}|{}]",action, splitId, hostnames, splitStartKey, splitStopKey);
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
	 * @param startKey
	 *        Start key of the region
	 * @param endKey
	 *        End key of the region
	 * @return true, if this region needs to be included as part of the input (default).
	 */
	private static boolean includeRegionInSplit(final byte[] startKey, final byte[] endKey) {
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