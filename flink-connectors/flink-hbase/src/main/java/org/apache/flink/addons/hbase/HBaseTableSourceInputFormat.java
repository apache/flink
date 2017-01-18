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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HRegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * {@link InputFormat} subclass that wraps the access for HTables. Returns the result as {@link Row}
 */
public class HBaseTableSourceInputFormat extends RichInputFormat<Row, TableInputSplit> implements ResultTypeQueryable<Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HBaseTableSourceInputFormat.class);
	private String tableName;
	private TypeInformation[] fieldTypeInfos;
	private String[] fieldNames;
	private transient Table table;
	private transient Scan scan;
	private transient Connection conn;
	private ResultScanner resultScanner = null;

	private byte[] lastRow;
	private int scannedRows;
	private boolean endReached = false;
	private org.apache.hadoop.conf.Configuration conf;
	private static final String COLON = ":";

	public HBaseTableSourceInputFormat(org.apache.hadoop.conf.Configuration conf, String tableName, String[] fieldNames, TypeInformation[] fieldTypeInfos) {
		this.conf = conf;
		this.tableName = tableName;
		this.fieldNames = fieldNames;
		this.fieldTypeInfos = fieldTypeInfos;
	}

	@Override
	public void configure(Configuration parameters) {
		LOG.info("Initializing HBaseConfiguration");
		connectToTable();
		if(table != null) {
			scan = createScanner();
		}
	}

	private Scan createScanner() {
		Scan scan = new Scan();
		for(String field : fieldNames) {
			// select only the fields in the 'selectedFields'
			String[] famCol = field.split(COLON);
			scan.addColumn(Bytes.toBytes(famCol[0]), Bytes.toBytes(famCol[1]));
		}
		return scan;
	}

	private void connectToTable() {
		//use files found in the classpath
		if(this.conf == null) {
			this.conf = HBaseConfiguration.create();
		}
		try {
			conn = ConnectionFactory.createConnection(this.conf);
		} catch(IOException ioe) {
			LOG.error("Exception while creating connection to hbase cluster", ioe);
			return;
		}
		try {
			table = conn.getTable(TableName.valueOf(tableName));
		} catch(TableNotFoundException tnfe) {
			LOG.error("The table " + tableName + " not found ", tnfe);
		} catch(IOException ioe) {
			LOG.error("Exception while connecting to the table "+tableName+ " ", ioe);
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
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
		HRegionLocator regionLocator = new HRegionLocator(table.getName(), (ClusterConnection) conn);
		final Pair<byte[][], byte[][]> keys = regionLocator.getStartEndKeys();
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
			final String regionLocation = regionLocator.getRegionLocation(startKey, false).getHostnamePort();
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
				final TableInputSplit split = new TableInputSplit(id, hosts, table.getName().getName(), splitStart, splitStop);
				splits.add(split);
			}
		}
		LOG.info("Created " + splits.size() + " splits");
		for (TableInputSplit split : splits) {
			logSplitInfo("created", split);
		}
		return splits.toArray(new TableInputSplit[0]);
	}

	protected boolean includeRegionInSplit(final byte[] startKey, final byte[] endKey) {
		return true;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(TableInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
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
		// set the start row and stop row from the splits
		scan.setStartRow(split.getStartRow());
		lastRow = split.getEndRow();
		scan.setStopRow(lastRow);

		resultScanner = table.getScanner(scan);
		endReached = false;
		scannedRows = 0;
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
	public Row nextRecord(Row reuse) throws IOException {
		if (resultScanner == null) {
			throw new IOException("No table result scanner provided!");
		}
		try {
			Result res = resultScanner.next();
			if (res != null) {
				scannedRows++;
				lastRow = res.getRow();
				return mapResultToRow(res);
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
				return mapResultToRow(res);
			}
		}
		endReached = true;
		return null;
	}

	private Row mapResultToRow(Result res) {
		Object[] values = new Object[fieldNames.length];
		int i = 0;
		for(String field : fieldNames) {
			String[] famCol = field.split(COLON);
			byte[] value = res.getValue(Bytes.toBytes(famCol[0]), Bytes.toBytes(famCol[1]));
			TypeInformation typeInfo = fieldTypeInfos[i];
			if(typeInfo.isBasicType()) {
				if(typeInfo.getTypeClass() ==  Integer.class) {
					values[i] = Bytes.toInt(value);
				} else if(typeInfo.getTypeClass() == Short.class) {
					values[i] = Bytes.toShort(value);
				} else if(typeInfo.getTypeClass() == Float.class) {
					values[i] = Bytes.toFloat(value);
				} else if(typeInfo.getTypeClass() == Long.class) {
					values[i] = Bytes.toLong(value);
				} else if(typeInfo.getTypeClass() == String.class) {
					values[i] = Bytes.toString(value);
				} else if(typeInfo.getTypeClass() == Byte.class) {
					values[i] = value[0];
				} else if(typeInfo.getTypeClass() == Boolean.class) {
					values[i] = Bytes.toBoolean(value);
				} else if(typeInfo.getTypeClass() == Double.class) {
					values[i] = Bytes.toDouble(value);
				} else if(typeInfo.getTypeClass() == BigInteger.class) {
					values[i] = new BigInteger(value);
				} else if(typeInfo.getTypeClass() == BigDecimal.class) {
					values[i] = Bytes.toBigDecimal(value);
				} else if(typeInfo.getTypeClass() == Date.class) {
					values[i] = new Date(Bytes.toLong(value));
				}
			} else {
				// TODO for other types??
			}
			i++;
		}
		return Row.of(values);
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
	public TypeInformation<Row> getProducedType() {
		return new RowTypeInfo(this.fieldTypeInfos);
	}
}
