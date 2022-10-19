/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.sink2;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.sink2.statement.JdbcQueryStatement;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class JdbcWriter<IN> implements SinkWriter<IN> {

    private final Deque<IN> bufferedRequests = new ArrayDeque<>();
    private final JdbcConnectionProvider connectionProvider;
    private final JdbcExecutionOptions executionOptions;
    private final JdbcQueryStatement<IN> queryStatement;
    private transient PreparedStatement preparedStatement;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> schedulerFuture;
    private transient volatile Exception schedulerException;

    JdbcWriter(
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executionOptions,
            JdbcQueryStatement<IN> queryStatement)
            throws IOException {
        this.connectionProvider = connectionProvider;
        this.executionOptions = executionOptions;
        this.queryStatement = queryStatement;

        open();
    }

    private void open() throws IOException {
        try {
            Connection connection = connectionProvider.getOrEstablishConnection();
            if (connection == null) {
                throw new IOException("cant establish connection");
            }
            preparedStatement = connection.prepareStatement(queryStatement.query());
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
        this.createScheduler();
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        checkSchedulerException();

        bufferedRequests.add(element);

        if (executionOptions.getBatchSize() > 0
                && executionOptions.getBatchSize() >= bufferedRequests.size()) {
            flush();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        flush();
    }

    private void flush() throws IOException, InterruptedException {
        checkSchedulerException();

        if (bufferedRequests.isEmpty()) {
            return;
        }
        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                List<IN> entries = new ArrayList<>(bufferedRequests);
                for (IN elem : entries) {
                    queryStatement.map(preparedStatement, elem);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                bufferedRequests.clear();
            } catch (Exception e) {
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        this.open();
                    }
                } catch (Exception exception) {
                    throw new IOException("Reestablish JDBC connection failed", exception);
                }
                try {
                    Thread.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (this.schedulerFuture != null) {
            this.schedulerFuture.cancel(false);
            this.scheduler.shutdown();
        }

        if (this.bufferedRequests.size() > 0) {
            flush();
        }

        if (this.preparedStatement != null) {
            preparedStatement.close();
            preparedStatement = null;
        }

        connectionProvider.closeConnection();

        checkSchedulerException();
    }

    private void createScheduler() {
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("jdbc-upsert-sink"));
            this.schedulerFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (this) {
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        this.schedulerException = e;
                                    }
                                }
                            },
                            executionOptions.getBatchIntervalMs(),
                            executionOptions.getBatchIntervalMs(),
                            TimeUnit.MILLISECONDS);
        }
    }

    private void checkSchedulerException() {
        if (schedulerException != null) {
            throw new RuntimeException("Writing records to JDBC failed.", schedulerException);
        }
    }
}
