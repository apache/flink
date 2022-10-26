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
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * OutputFormat to write Flink {@link Row}s into a Cassandra cluster. * Please read the
 * recommendations in {@linkplain CassandraOutputFormatBase}.
 */
public class CassandraRowOutputFormat extends CassandraColumnarOutputFormatBase<Row> {

    public CassandraRowOutputFormat(String insertQuery, ClusterBuilder builder) {
        this(insertQuery, builder, Integer.MAX_VALUE, Duration.ofMillis(Long.MAX_VALUE));
    }

    public CassandraRowOutputFormat(
            String insertQuery,
            ClusterBuilder builder,
            int maxConcurrentRequests,
            Duration maxConcurrentRequestsTimeout) {
        super(insertQuery, builder, maxConcurrentRequests, maxConcurrentRequestsTimeout);
    }

    @Override
    protected Object[] extractFields(Row record) {

        Object[] fields = new Object[record.getArity()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = record.getField(i);
        }
        return fields;
    }
}
