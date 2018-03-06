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

package org.apache.flink.addons.hbase.strategy;

import org.apache.flink.addons.hbase.TableInputSplit;
import org.apache.flink.addons.hbase.util.HBaseConnectorUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Table input split strategy for traditional HTable scanning.
 */
public class TableInputSplitStrategyImpl extends AbstractTableInputSplitStrategy
	implements TableInputSplitStrategy {

	protected static final Logger LOG = LoggerFactory.getLogger(TableInputSplitStrategyImpl.class);

	@Override
	public TableInputSplit[] createInputSplits(Configuration hbaseConfiguration, int minNumSplits) throws IOException {
		preCheck();

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
			HBaseConnectorUtil.logSplitInfo("created", split, this.getClass().getName());
		}
		return splits.toArray(new TableInputSplit[splits.size()]);
	}

	@Override
	public ResultScanner createResultScanner(Configuration hbaseConfiguration, TableInputSplit split) throws IOException {
		return table.getScanner(scan);
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
}
