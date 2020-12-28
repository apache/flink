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

package org.apache.flink.connector.hbase2.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.source.TableInputSplit;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.IOUtils;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Abstract {@link InputFormat} to read data from HBase tables. */
@Internal
public abstract class AbstractTableInputFormat<T> extends RichInputFormat<T, TableInputSplit> {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractTableInputFormat.class);
    private static final long serialVersionUID = 1L;

    // helper variable to decide whether the input is exhausted or not
    protected boolean endReached = false;

    protected transient Connection connection = null;
    protected transient Table table = null;
    protected transient RegionLocator regionLocator = null;
    protected transient Scan scan = null;

    /** HBase iterator wrapper. */
    protected ResultScanner resultScanner = null;

    protected byte[] currentRow;
    protected long scannedRows;

    // Configuration is not serializable
    protected byte[] serializedConfig;

    public AbstractTableInputFormat(org.apache.hadoop.conf.Configuration hConf) {
        serializedConfig = HBaseConfigurationUtil.serializeConfiguration(hConf);
    }

    /**
     * Creates a {@link Scan} object and opens the {@link HTable} connection to initialize the HBase
     * table.
     *
     * @throws IOException Thrown, if the connection could not be opened due to an I/O problem.
     */
    protected abstract void initTable() throws IOException;

    /**
     * Returns an instance of Scan that retrieves the required subset of records from the HBase
     * table.
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

    @Override
    public void configure(Configuration parameters) {}

    protected org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        return HBaseConfigurationUtil.deserializeConfiguration(
                serializedConfig, HBaseConfigurationUtil.getHBaseConfiguration());
    }

    /**
     * Creates a {@link Scan} object and opens the {@link HTable} connection. The connection is
     * opened in this method and closed in {@link #close()}.
     *
     * @param split The split to be opened.
     * @throws IOException Thrown, if the spit could not be opened due to an I/O problem.
     */
    @Override
    public void open(TableInputSplit split) throws IOException {
        initTable();

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

    @Override
    public T nextRecord(T reuse) throws IOException {
        if (resultScanner == null) {
            throw new IOException("No table result scanner provided!");
        }
        Result res;
        try {
            res = resultScanner.next();
        } catch (Exception e) {
            resultScanner.close();
            // workaround for timeout on scan
            LOG.warn(
                    "Error after scan of " + scannedRows + " rows. Retry with a new scanner...", e);
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
        LOG.info(
                "{} split (this={})[{}|{}|{}|{}]",
                action,
                this,
                splitId,
                hostnames,
                splitStartKey,
                splitStopKey);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return endReached;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing split (scanned {} rows)", scannedRows);
        currentRow = null;
        IOUtils.closeQuietly(resultScanner);
        resultScanner = null;
        closeTable();
    }

    public void closeTable() {
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                LOG.warn("Exception occurs while closing HBase Table.", e);
            }
            table = null;
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                LOG.warn("Exception occurs while closing HBase Connection.", e);
            }
            connection = null;
        }
    }

    @Override
    public TableInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
        try {
            initTable();

            // Get the starting and ending row keys for every region in the currently open table
            final Pair<byte[][], byte[][]> keys = regionLocator.getStartEndKeys();
            if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
                LOG.warn(
                        "Unexpected region keys: {} appeared in HBase table: {}, all region information are: {}.",
                        keys,
                        table,
                        regionLocator.getAllRegionLocations());
                throw new IOException(
                        "HBase Table expects at least one region in scan,"
                                + " please check the HBase table status in HBase cluster");
            }
            final byte[] startRow = scan.getStartRow();
            final byte[] stopRow = scan.getStopRow();
            final boolean scanWithNoLowerBound = startRow.length == 0;
            final boolean scanWithNoUpperBound = stopRow.length == 0;

            final List<TableInputSplit> splits = new ArrayList<>(minNumSplits);
            for (int i = 0; i < keys.getFirst().length; i++) {
                final byte[] startKey = keys.getFirst()[i];
                final byte[] endKey = keys.getSecond()[i];
                final String regionLocation =
                        regionLocator.getRegionLocation(startKey, false).getHostnamePort();
                // Test if the given region is to be included in the InputSplit while splitting the
                // regions of a table
                if (!includeRegionInScan(startKey, endKey)) {
                    continue;
                }
                // Find the region on which the given row is being served
                final String[] hosts = new String[] {regionLocation};

                // Determine if regions contains keys used by the scan
                boolean isLastRegion = endKey.length == 0;
                if ((scanWithNoLowerBound || isLastRegion || Bytes.compareTo(startRow, endKey) < 0)
                        && (scanWithNoUpperBound || Bytes.compareTo(stopRow, startKey) > 0)) {

                    final byte[] splitStart =
                            scanWithNoLowerBound || Bytes.compareTo(startKey, startRow) >= 0
                                    ? startKey
                                    : startRow;
                    final byte[] splitStop =
                            (scanWithNoUpperBound || Bytes.compareTo(endKey, stopRow) <= 0)
                                            && !isLastRegion
                                    ? endKey
                                    : stopRow;
                    int id = splits.size();
                    final TableInputSplit split =
                            new TableInputSplit(
                                    id, hosts, table.getName().getName(), splitStart, splitStop);
                    splits.add(split);
                }
            }
            LOG.info("Created " + splits.size() + " splits");
            for (TableInputSplit split : splits) {
                logSplitInfo("created", split);
            }
            return splits.toArray(new TableInputSplit[splits.size()]);
        } finally {
            closeTable();
        }
    }

    /**
     * Test if the given region is to be included in the scan while splitting the regions of a
     * table.
     *
     * @param startKey Start key of the region
     * @param endKey End key of the region
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

    @VisibleForTesting
    public Connection getConnection() {
        return connection;
    }
}
