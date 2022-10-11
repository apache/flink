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

package org.apache.flink.connector.cassandra.source.reader;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.CassandraSplitState;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import java.util.Map;

/**
 * Cassandra {@link SourceReader} that reads one {@link CassandraSplit} using a single thread.
 * Indeed, this is for reducing the load on Cassandra cluster (see {@link CassandraSplit}).
 *
 * @param <OUT> the type of elements produced by the source
 */
public class CassandraSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                CassandraRow, OUT, CassandraSplit, CassandraSplitState> {

    public CassandraSourceReader(
            SourceReaderContext context,
            ClusterBuilder clusterBuilder,
            Class<OUT> pojoClass,
            String query,
            MapperOptions mapperOptions) {
        super(
                () -> new CassandraSplitReader(clusterBuilder, query),
                new CassandraRecordEmitter<>(pojoClass, clusterBuilder, mapperOptions),
                new Configuration(),
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, CassandraSplitState> finishedSplitIds) {
        finishedSplitIds.forEach((key, splitState) -> splitState.markAllRingRangesAsFinished());
    }

    @Override
    protected CassandraSplitState initializedState(CassandraSplit cassandraSplit) {
        // no need to deep copy the ringRanges as this method is called only by SourceReaderBase,
        // there will be no serialization involved
        return new CassandraSplitState(cassandraSplit.getRingRanges(), cassandraSplit.splitId());
    }

    @Override
    protected CassandraSplit toSplitType(String splitId, CassandraSplitState splitState) {
        // no need to deep copy the ringRanges as this method is called only by SourceReaderBase,
        // there will be no serialization involved
        return new CassandraSplit(splitState.getUnprocessedRingRanges());
    }
}
