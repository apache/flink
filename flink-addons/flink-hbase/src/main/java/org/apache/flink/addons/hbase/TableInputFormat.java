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

import org.apache.flink.addons.hbase.common.HBaseKey;
import org.apache.flink.addons.hbase.common.HBaseResult;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Record;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link InputFormat} subclass that wraps the access for HTables.
 */
public class TableInputFormat implements InputFormat<Record, TableInputSplit> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(TableInputFormat.class);

	/** Hbase' iterator wrapper */
	private TableRecordReader tableRecordReader;

	/** helper variable to decide whether the input is exhausted or not */
	private boolean endReached = false;

	/** Job parameter that specifies the input table. */
	public static final String INPUT_TABLE = "hbase.inputtable";

	/** Column Family to Scan */
	public static final String SCAN_COLUMN_FAMILY = "hbase.scan.column.family";

	/** Space delimited list of columns to scan. */
	public static final String SCAN_COLUMNS = "hbase.scan.columns";

	/** The timestamp used to filter columns with a specific timestamp. */
	public static final String SCAN_TIMESTAMP = "hbase.scan.timestamp";

	/** The starting timestamp used to filter columns with a specific range of versions. */
	public static final String SCAN_TIMERANGE_START = "hbase.scan.timerange.start";

	/** The ending timestamp used to filter columns with a specific range of versions. */
	public static final String SCAN_TIMERANGE_END = "hbase.scan.timerange.end";

	/** The maximum number of version to return. */
	public static final String SCAN_MAXVERSIONS = "hbase.scan.maxversions";

	/** Set to false to disable server-side caching of blocks for this scan. */
	public static final String SCAN_CACHEBLOCKS = "hbase.scan.cacheblocks";

	/** The number of rows for caching that will be passed to scanners. */
	public static final String SCAN_CACHEDROWS = "hbase.scan.cachedrows";

	/** mutable objects that are used to avoid recreation of wrapper objects */
	protected HBaseKey hbaseKey;

	protected HBaseResult hbaseResult;

	private Configuration parameters;

//	private org.apache.hadoop.conf.Configuration hConf;
	public TableInputFormat(Configuration parameters){
		this.parameters = parameters;
	}
	
	@Override
	public void configure(Configuration parameters) {
		//TODO why parameters gets empty after execution??
	}

	/**
	 * Read the configuration and creates a {@link Scan} object.
	 * 
	 * @param parameters
	 * @return The scanner
	 */
	protected Scan createScanner(Configuration parameters) {
		Scan scan = null;
		try {
			scan = new Scan();

			// if (parameters.getString(SCAN_COLUMNS, null) != null) {
			// scan.addColumns(parameters.getString(SCAN_COLUMNS, null));
			// }

			if (parameters.getString(SCAN_COLUMN_FAMILY, null) != null) {
				scan.addFamily(Bytes.toBytes(parameters.getString(SCAN_COLUMN_FAMILY, null)));
			}

			if (parameters.getString(SCAN_TIMESTAMP, null) != null) {
				scan.setTimeStamp(Long.parseLong(parameters.getString(SCAN_TIMESTAMP, null)));
			}

			if (parameters.getString(SCAN_TIMERANGE_START, null) != null
				&& parameters.getString(SCAN_TIMERANGE_END, null) != null) {
				scan.setTimeRange(
					Long.parseLong(parameters.getString(SCAN_TIMERANGE_START, null)),
					Long.parseLong(parameters.getString(SCAN_TIMERANGE_END, null)));
			}

			if (parameters.getString(SCAN_MAXVERSIONS, null) != null) {
				scan.setMaxVersions(Integer.parseInt(parameters.getString(SCAN_MAXVERSIONS, null)));
			}

			if (parameters.getString(SCAN_CACHEDROWS, null) != null) {
				scan.setCaching(Integer.parseInt(parameters.getString(SCAN_CACHEDROWS, null)));
			}

			// false by default, full table scans generate too much BC churn
			scan.setCacheBlocks((parameters.getBoolean(SCAN_CACHEBLOCKS, false)));
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}
		return scan;
	}

	/**
	 * Create an {@link HTable} instance and set it into this format.
	 * 
	 * @param parameters
	 *        a {@link Configuration} that holds at least the table name.
	 */
	protected HTable createTable(Configuration parameters) {
		LOG.info("Initializing HBaseConfiguration");
		//use files found in the classpath
		org.apache.hadoop.conf.Configuration hConf = HBaseConfiguration.create();
		
		String tableName = parameters.getString(INPUT_TABLE, "");
		try {
			return new HTable(hConf, tableName);
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}
		return null;
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.endReached;
	}

	protected boolean nextResult() throws IOException {
		if (this.tableRecordReader == null)
		{
			throw new IOException("No table record reader provided!");
		}

		try {
			if (this.tableRecordReader.nextKeyValue())
			{
				ImmutableBytesWritable currentKey = this.tableRecordReader.getCurrentKey();
				Result currentValue = this.tableRecordReader.getCurrentValue();

				hbaseKey.setWritable(currentKey);
				hbaseResult.setResult(currentValue);
			} else
			{
				this.endReached = true;
				return false;
			}
		} catch (InterruptedException e) {
			LOG.error("Table reader has been interrupted", e);
			throw new IOException(e);
		}

		return true;
	}

	@Override
	public Record nextRecord(Record record) throws IOException {
		if (nextResult()) {
			mapResultToRecord(record, hbaseKey, hbaseResult);
			return record;
		} else {
			return null;
		}
	}

	/**
	 * Maps the current HBase Result into a Record.
	 * This implementation simply stores the HBaseKey at position 0, and the HBase Result object at position 1.
	 * 
	 * @param record
	 * @param key
	 * @param result
	 */
	public void mapResultToRecord(Record record, HBaseKey key, HBaseResult result) {
		record.setField(0, key);
		record.setField(1, result);
	}

	@Override
	public void close() throws IOException {
		this.tableRecordReader.close();
	}

	@Override
	public void open(TableInputSplit split) throws IOException {
		HTable table = createTable(parameters);
		if (split == null)
		{
			throw new IOException("Input split is null!");
		}

		this.tableRecordReader = new TableRecordReader();

		this.tableRecordReader.setHTable(table);

		Scan sc = createScanner(parameters);
		sc.setStartRow(split.getStartRow());
		LOG.info("split start row: " + new String(split.getStartRow()));
		sc.setStopRow(split.getEndRow());
		LOG.info("split end row: " + new String(split.getEndRow()));

		this.tableRecordReader.setScan(sc);
		this.tableRecordReader.restart(split.getStartRow());

		this.hbaseKey = new HBaseKey();
		this.hbaseResult = new HBaseResult();

		endReached = false;
	}


	@Override
	public TableInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
		HTable table = createTable(parameters);

		final Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
			throw new IOException("Expecting at least one region.");
		}
		Scan scan = createScanner(parameters);
		int count = 0;
		final List<TableInputSplit> splits = new ArrayList<TableInputSplit>(keys.getFirst().length);
		for (int i = 0; i < keys.getFirst().length; i++) {

			if (!includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
				continue;
			}

			final String regionLocation = table.getRegionLocation(keys.getFirst()[i], false).getHostnamePort();
			final byte[] startRow = scan.getStartRow();
			final byte[] stopRow = scan.getStopRow();

			// determine if the given start an stop key fall into the region
			if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
				Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
				(stopRow.length == 0 ||
				Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {

				final byte[] splitStart = startRow.length == 0 ||
					Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
					keys.getFirst()[i] : startRow;
				final byte[] splitStop = (stopRow.length == 0 ||
					Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
					keys.getSecond()[i].length > 0 ?
					keys.getSecond()[i] : stopRow;
				final TableInputSplit split = new TableInputSplit(splits.size(), new String[] { regionLocation },
					table.getTableName(), splitStart, splitStop);
				splits.add(split);
				if (LOG.isDebugEnabled()) {
					LOG.debug("getSplits: split -> " + (count++) + " -> " + split);
				}
			}
		}

		return splits.toArray(new TableInputSplit[0]);
	}

	/**
	 * Test if the given region is to be included in the InputSplit while splitting
	 * the regions of a table.
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

}
