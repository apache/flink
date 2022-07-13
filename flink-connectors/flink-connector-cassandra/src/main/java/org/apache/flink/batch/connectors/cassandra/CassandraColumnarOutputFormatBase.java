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

import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * CassandraColumnarOutputFormatBase is the common abstract class for writing into Apache Cassandra
 * using column based output formats.
 *
 * @param <OUT> Type of the elements to write.
 */
abstract class CassandraColumnarOutputFormatBase<OUT>
        extends CassandraOutputFormatBase<OUT, ResultSet> {
    private final String insertQuery;
    private transient PreparedStatement prepared;

    public CassandraColumnarOutputFormatBase(
            String insertQuery,
            ClusterBuilder builder,
            int maxConcurrentRequests,
            Duration maxConcurrentRequestsTimeout) {
        super(builder, maxConcurrentRequests, maxConcurrentRequestsTimeout);
        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(insertQuery), "Query cannot be null or empty");
        this.insertQuery = insertQuery;
    }

    @Override
    protected void postOpen() {
        super.postOpen();
        this.prepared = session.prepare(insertQuery);
    }

    @Override
    protected CompletionStage<ResultSet> send(OUT record) {
        Object[] fields = extractFields(record);
        final ResultSetFuture result = session.executeAsync(prepared.bind(fields));
        return listenableFutureToCompletableFuture(result);
    }

    protected abstract Object[] extractFields(OUT record);
}
