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

package org.apache.flink.connector.hbase.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.HBaseEvent;
import org.apache.flink.connector.hbase.sink.HBaseSinkOptions;
import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;

import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.io.Closer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * The HBaseWriter is responsible for writing incoming events to HBase.
 *
 * <p>Stored events will be flushed to HBase eiter if the {@link #queueLimit} is reached or if the
 * {@link #maxLatencyMs} has elapsed.
 */
@Internal
public class HBaseWriter<IN> implements SinkWriter<IN, Void, Mutation> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseWriter.class);

    private final int queueLimit;
    private final int maxLatencyMs;
    private final HBaseSinkSerializer<IN> sinkSerializer;
    private final ArrayBlockingQueue<Mutation> pendingMutations;
    private final Connection connection;
    private final Table table;
    private volatile long lastFlushTimeStamp = 0;
    private TimerTask batchSendTimer;

    public HBaseWriter(
            Sink.InitContext context,
            List<Mutation> states,
            HBaseSinkSerializer<IN> sinkSerializer,
            byte[] serializedHBaseConfig,
            Configuration sinkConfiguration) {
        this.sinkSerializer = sinkSerializer;
        this.queueLimit = sinkConfiguration.get(HBaseSinkOptions.QUEUE_LIMIT);
        this.maxLatencyMs = sinkConfiguration.get(HBaseSinkOptions.MAX_LATENCY);
        String tableName = sinkConfiguration.get(HBaseSinkOptions.TABLE_NAME);

        // Queue limit is multiplied by 2, to reduce the probability of blocking while committing
        this.pendingMutations = new ArrayBlockingQueue<>(2 * queueLimit);
        pendingMutations.addAll(states);

        org.apache.hadoop.conf.Configuration hbaseConfiguration =
                HBaseConfigurationUtil.deserializeConfiguration(serializedHBaseConfig, null);
        try {
            connection = ConnectionFactory.createConnection(hbaseConfiguration);
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException("Connection to HBase couldn't be established", e);
        }

        startBatchSendTimer();
        LOG.debug("started sink writer for table {}", tableName);
    }

    private void startBatchSendTimer() {
        batchSendTimer =
                new TimerTask() {
                    @Override
                    public void run() {
                        long diff = System.currentTimeMillis() - lastFlushTimeStamp;
                        if (diff > maxLatencyMs) {
                            LOG.debug("Time based flushing of mutations");
                            flushBuffer();
                        }
                    }
                };
        new Timer().scheduleAtFixedRate(batchSendTimer, 0, maxLatencyMs / 2);
    }

    private void flushBuffer() {
        lastFlushTimeStamp = System.currentTimeMillis();
        if (pendingMutations.size() == 0) {
            return;
        }
        try {
            ArrayList<Mutation> batch = new ArrayList<>();
            pendingMutations.drainTo(batch);
            table.batch(batch, null);
            pendingMutations.clear();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed storing batch data in HBase", e);
        }
    }

    @Override
    public void write(IN element, Context context) {
        HBaseEvent event = sinkSerializer.serialize(element);
        if (event.getType() == Cell.Type.Put) {
            Put put = new Put(event.getRowIdBytes());
            put.addColumn(
                    event.getColumnFamilyBytes(), event.getQualifierBytes(), event.getPayload());
            pendingMutations.add(put);
        } else if (event.getType() == Cell.Type.Delete) {
            Delete delete = new Delete(event.getRowIdBytes());
            delete.addColumn(event.getColumnFamilyBytes(), event.getQualifierBytes());
            pendingMutations.add(delete);
        } else {
            throw new UnsupportedOperationException("event type not supported");
        }

        if (pendingMutations.size() >= queueLimit) {
            LOG.debug("Capacity based flushing of mutations");
            flushBuffer();
        }
    }

    @Override
    public List<Void> prepareCommit(boolean flush) {
        return Collections.emptyList();
    }

    @Override
    public List<Mutation> snapshotState() {
        ArrayList<Mutation> snapshot = new ArrayList<>();
        pendingMutations.drainTo(snapshot);
        LOG.debug("Snapshotting state with {} elements", snapshot.size());
        return snapshot;
    }

    @Override
    public void close() throws Exception {
        try (Closer closer = Closer.create()) {
            closer.register(connection);
            closer.register(table);
            closer.register(this::flushBuffer);
            closer.register(batchSendTimer::cancel);
        }
    }
}
