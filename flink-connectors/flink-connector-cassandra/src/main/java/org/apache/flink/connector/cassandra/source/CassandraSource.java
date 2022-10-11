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

package org.apache.flink.connector.cassandra.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.cassandra.source.enumerator.CassandraEnumeratorState;
import org.apache.flink.connector.cassandra.source.enumerator.CassandraEnumeratorStateSerializer;
import org.apache.flink.connector.cassandra.source.enumerator.CassandraSplitEnumerator;
import org.apache.flink.connector.cassandra.source.reader.CassandraSourceReader;
import org.apache.flink.connector.cassandra.source.reader.CassandraSplitReader;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.CassandraSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A bounded source to read from Cassandra and return a collection of entities as {@code
 * DataStream<Entity>}. An entity is built by Cassandra mapper ({@code
 * com.datastax.driver.mapping.EntityMapper}) based on a POJO containing annotations (as described
 * in <a
 * href="https://docs.datastax.com/en/developer/java-driver/3.11/manual/object_mapper/creating/">
 * Cassandra object mapper</a>).
 *
 * <p>To use it, do the following:
 *
 * <pre>{@code
 * ClusterBuilder clusterBuilder = new ClusterBuilder() {
 *   @Override
 *   protected Cluster buildCluster(Cluster.Builder builder) {
 *     return builder.addContactPointsWithPorts(new InetSocketAddress(HOST,PORT))
 *                   .withQueryOptions(new QueryOptions().setConsistencyLevel(CL))
 *                   .withSocketOptions(new SocketOptions()
 *                   .setConnectTimeoutMillis(CONNECT_TIMEOUT)
 *                   .setReadTimeoutMillis(READ_TIMEOUT))
 *                   .build();
 *   }
 * };
 * Source cassandraSource = new CassandraSource(clusterBuilder,
 *                                              Pojo.class,
 *                                              "select ... from KEYSPACE.TABLE ...;",
 *                                              () -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)});
 *
 * DataStream<Pojo> stream = env.fromSource(cassandraSource, WatermarkStrategy.noWatermarks(),
 * "CassandraSource");
 * }</pre>
 */
@PublicEvolving
public class CassandraSource<OUT>
        implements Source<OUT, CassandraSplit, CassandraEnumeratorState>, ResultTypeQueryable<OUT> {

    public static final String CQL_PROHIBITTED_CLAUSES_REGEXP =
            "(?i).*(AVG|COUNT|MIN|MAX|SUM|ORDER|GROUP BY).*";
    private static final long serialVersionUID = 7773196541275567433L;

    private final ClusterBuilder clusterBuilder;
    private final Class<OUT> pojoClass;
    private final String query;
    private final MapperOptions mapperOptions;

    public CassandraSource(
            ClusterBuilder clusterBuilder,
            Class<OUT> pojoClass,
            String query,
            MapperOptions mapperOptions) {
        checkNotNull(clusterBuilder, "ClusterBuilder required but not provided");
        checkNotNull(pojoClass, "POJO class required but not provided");
        checkQueryValidity(query);
        this.clusterBuilder = clusterBuilder;
        ClosureCleaner.clean(clusterBuilder, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        this.pojoClass = pojoClass;
        this.query = query;
        this.mapperOptions = mapperOptions;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<OUT, CassandraSplit> createReader(SourceReaderContext readerContext) {
        return new CassandraSourceReader<>(
                readerContext, clusterBuilder, pojoClass, query, mapperOptions);
    }

    @Override
    public SplitEnumerator<CassandraSplit, CassandraEnumeratorState> createEnumerator(
            SplitEnumeratorContext<CassandraSplit> enumContext) {
        return new CassandraSplitEnumerator(enumContext, null, clusterBuilder);
    }

    @Override
    public SplitEnumerator<CassandraSplit, CassandraEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<CassandraSplit> enumContext,
            CassandraEnumeratorState enumCheckpoint) {
        return new CassandraSplitEnumerator(enumContext, enumCheckpoint, clusterBuilder);
    }

    @Override
    public SimpleVersionedSerializer<CassandraSplit> getSplitSerializer() {
        return CassandraSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<CassandraEnumeratorState> getEnumeratorCheckpointSerializer() {
        return CassandraEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return TypeInformation.of(pojoClass);
    }

    @VisibleForTesting
    public static void checkQueryValidity(String query) {
        checkNotNull(query, "query required but not provided");
        checkState(
                query.matches(CassandraSplitReader.SELECT_REGEXP),
                "query must be of the form select ... from keyspace.table ...;");
        checkState(
                !query.matches(CQL_PROHIBITTED_CLAUSES_REGEXP),
                "query must not contain aggregate or order clauses because they will be done per split. "
                        + "So they will be incorrect after merging the splits");
    }
}
