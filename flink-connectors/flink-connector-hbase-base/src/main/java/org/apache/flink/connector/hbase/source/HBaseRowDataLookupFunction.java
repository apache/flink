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

package org.apache.flink.connector.hbase.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseSerde;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * The HBaseRowDataLookupFunction is a standard user-defined table function, it can be used in
 * tableAPI and also useful for temporal table join plan in SQL. It looks up the result as {@link
 * RowData}.
 */
@Internal
public class HBaseRowDataLookupFunction extends LookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseRowDataLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final String hTableName;
    private final byte[] serializedConfig;
    private final HBaseTableSchema hbaseTableSchema;
    private final String nullStringLiteral;

    private transient Connection hConnection;
    private transient HTable table;
    private transient HBaseSerde serde;

    private final int maxRetryTimes;

    public HBaseRowDataLookupFunction(
            Configuration configuration,
            String hTableName,
            HBaseTableSchema hbaseTableSchema,
            String nullStringLiteral,
            int maxRetryTimes) {
        this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(configuration);
        this.hTableName = hTableName;
        this.hbaseTableSchema = hbaseTableSchema;
        this.nullStringLiteral = nullStringLiteral;
        this.maxRetryTimes = maxRetryTimes;
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param keyRow - A {@link RowData} that wraps lookup keys. Currently only support single
     *     rowkey.
     */
    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                // TODO: The implementation of LookupFunction will pass a GenericRowData as key row
                // and it's safe to cast for now. We need to update the logic once we improve the
                // LookupFunction in the future.
                Get get = serde.createGet(((GenericRowData) keyRow).getField(0));
                if (get != null) {
                    Result result = table.get(get);
                    if (!result.isEmpty()) {
                        return Collections.singletonList(serde.convertToReusedRow(result));
                    }
                }
                break;
            } catch (IOException e) {
                LOG.error(String.format("HBase lookup error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of HBase lookup failed.", e);
                }
                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
        return Collections.emptyList();
    }

    private Configuration prepareRuntimeConfiguration() {
        // create default configuration from current runtime env (`hbase-site.xml` in classpath)
        // first,
        // and overwrite configuration using serialized configuration from client-side env
        // (`hbase-site.xml` in classpath).
        // user params from client-side have the highest priority
        Configuration runtimeConfig =
                HBaseConfigurationUtil.deserializeConfiguration(
                        serializedConfig, HBaseConfigurationUtil.getHBaseConfiguration());

        // do validation: check key option(s) in final runtime configuration
        if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
            LOG.error(
                    "can not connect to HBase without {} configuration",
                    HConstants.ZOOKEEPER_QUORUM);
            throw new IllegalArgumentException(
                    "check HBase configuration failed, lost: '"
                            + HConstants.ZOOKEEPER_QUORUM
                            + "'!");
        }

        return runtimeConfig;
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        Configuration config = prepareRuntimeConfiguration();
        try {
            hConnection = ConnectionFactory.createConnection(config);
            table = (HTable) hConnection.getTable(TableName.valueOf(hTableName));
        } catch (TableNotFoundException tnfe) {
            LOG.error("Table '{}' not found ", hTableName, tnfe);
            throw new RuntimeException("HBase table '" + hTableName + "' not found.", tnfe);
        } catch (IOException ioe) {
            LOG.error("Exception while creating connection to HBase.", ioe);
            throw new RuntimeException("Cannot create connection to HBase.", ioe);
        }
        this.serde = new HBaseSerde(hbaseTableSchema, nullStringLiteral);
        LOG.info("end open.");
    }

    @Override
    public void close() {
        LOG.info("start close ...");
        if (null != table) {
            try {
                table.close();
                table = null;
            } catch (IOException e) {
                // ignore exception when close.
                LOG.warn("exception when close table", e);
            }
        }
        if (null != hConnection) {
            try {
                hConnection.close();
                hConnection = null;
            } catch (IOException e) {
                // ignore exception when close.
                LOG.warn("exception when close connection", e);
            }
        }
        LOG.info("end close.");
    }

    @VisibleForTesting
    public String getHTableName() {
        return hTableName;
    }
}
