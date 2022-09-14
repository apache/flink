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

package org.apache.flink.batch.connectors.cassandra;

import org.apache.flink.api.common.io.OutputFormatBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * CassandraOutputFormatBase is the common abstract class for writing into Apache Cassandra using
 * output formats.
 *
 * <p>In case of experiencing the following error: {@code Error while sending value.
 * com.datastax.driver.core.exceptions.WriteTimeoutException: Cassandra timeout during write query
 * at consistency LOCAL_ONE (1 replica were required but only 0 acknowledged the write)},
 *
 * <p>it is recommended to increase the Cassandra write timeout to adapt to your workload in your
 * Cassandra cluster so that such timeout errors do not happen. For that you need to raise
 * write_request_timeout_in_ms conf parameter in your cassandra.yml. Indeed, This exception means
 * that Cassandra coordinator node (internal Cassandra) waited too long for an internal replication
 * (replication to another node and did not ack the write. It is not recommended to lower the
 * replication factor in your Cassandra cluster because it is mandatory that you do not loose data
 * in case of a Cassandra cluster failure. Waiting for a single replica for write acknowledge is the
 * minimum level for this guarantee in Cassandra.}
 *
 * @param <OUT> Type of the elements to write.
 */
abstract class CassandraOutputFormatBase<OUT, V> extends OutputFormatBase<OUT, V> {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraOutputFormatBase.class);

    private final ClusterBuilder builder;
    private transient Cluster cluster;
    protected transient Session session;

    public CassandraOutputFormatBase(
            ClusterBuilder builder,
            int maxConcurrentRequests,
            Duration maxConcurrentRequestsTimeout) {
        super(maxConcurrentRequests, maxConcurrentRequestsTimeout);
        Preconditions.checkNotNull(builder, "Builder cannot be null");
        this.builder = builder;
    }

    /** Configure the connection to Cassandra. */
    @Override
    public void configure(Configuration parameters) {
        this.cluster = builder.getCluster();
    }

    /** Opens a Session to Cassandra . */
    @Override
    protected void postOpen() {
        this.session = cluster.connect();
    }

    /** Closes all resources used by Cassandra connection. */
    @Override
    protected void postClose() {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing session.", e);
        }
        try {
            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing cluster.", e);
        }
    }

    protected static <T> CompletableFuture<T> listenableFutureToCompletableFuture(
            final ListenableFuture<T> listenableFuture) {
        CompletableFuture<T> completable = new CompletableFuture<T>();
        Futures.addCallback(listenableFuture, new CompletableFutureCallback<>(completable));
        return completable;
    }

    private static class CompletableFutureCallback<T> implements FutureCallback<T> {

        private final CompletableFuture<T> completableFuture;

        public CompletableFutureCallback(CompletableFuture<T> completableFuture) {
            this.completableFuture = completableFuture;
        }

        @Override
        public void onSuccess(@Nullable T result) {
            completableFuture.complete(result);
        }

        @Override
        public void onFailure(Throwable throwable) {
            completableFuture.completeExceptionally(throwable);
        }
    }
}
