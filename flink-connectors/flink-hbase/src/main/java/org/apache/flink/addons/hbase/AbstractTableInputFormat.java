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

import org.apache.flink.addons.hbase.strategy.TableInputSplitStrategy;
import org.apache.flink.addons.hbase.util.HBaseConnectorUtil;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Abstract {@link InputFormat} to read data from HBase tables.
 */
public abstract class AbstractTableInputFormat<T> extends RichInputFormat<T, TableInputSplit>
	implements HBaseTableScannerAware {

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
	 * By providing {@link #tableInputSplitStrategy} as a specific implementation, the subclass
	 * can perform separately for HTable scan and HTable snapshot scan.
	 */
	protected TableInputSplitStrategy tableInputSplitStrategy;

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
	 * Configuration is left to subclass to perform, such as creating
	 * a {@link Scan} object and opens the {@link HTable} connection.
	 *
	 * <p>Table InputSplit Strategy is configured.
	 *
	 * @param parameters The configuration that is to be used
	 * @see Configuration
	 */
	@Override
	public void configure(Configuration parameters) {
		try {
			Preconditions.checkNotNull(table, "Table should be initiated first by " +
				"overriding configure() method.");
			scan = getScanner();
			tableInputSplitStrategy.configure(table, scan);
		} catch (IOException e) {
			throw new RuntimeException("Configure table input split strategy failed due " +
				"to " + e.getMessage(), e);
		}
	}

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

		HBaseConnectorUtil.logSplitInfo("opening", split, this.getClass().getName());

		// set scan range
		currentRow = split.getStartRow();
		scan.setStartRow(currentRow);
		scan.setStopRow(split.getEndRow());

		resultScanner = tableInputSplitStrategy.createResultScanner(table.getConfiguration(), split);
		endReached = false;
		scannedRows = 0;
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
				currentRow = res.getRow();
				return mapResultToOutType(res);
			}
		} catch (Exception e) {
			resultScanner.close();
			//workaround for timeout on scan
			LOG.warn("Error after scan of " + scannedRows + " rows. Retry with a new scanner...", e);
			scan.setStartRow(currentRow);
			resultScanner = table.getScanner(scan);
			Result res = resultScanner.next();
			if (res != null) {
				scannedRows++;
				currentRow = res.getRow();
				return mapResultToOutType(res);
			}
		}

		endReached = true;
		return null;
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
	public TableInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return tableInputSplitStrategy.createInputSplits(table.getConfiguration(), minNumSplits);
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
