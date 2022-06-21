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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

/**
 * CassandraOutputFormatBase is the common abstract class for writing into Apache Cassandra using
 * output formats.
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
    public void open(int taskNumber, int numTasks) {
        this.session = cluster.connect();
        super.open(taskNumber, numTasks);
    }

    /** Closes all resources used by Cassandra connection. */
    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
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
    }
}
