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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.cassandra.source.split.CassandraSplitState;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * {@link RecordEmitter} that converts the {@link CassandraRow} read by the {@link
 * CassandraSplitReader} to specified POJO and output it while updating splits state. This class
 * uses the Cassandra driver mapper to map the row to the POJO.
 *
 * @param <OUT> type of POJO record to output
 */
public class CassandraRecordEmitter<OUT>
        implements RecordEmitter<CassandraRow, OUT, CassandraSplitState> {

    private final Mapper<OUT> mapper;

    public CassandraRecordEmitter(
            Class<OUT> pojoClass, ClusterBuilder clusterBuilder, MapperOptions mapperOptions) {
        // session and cluster are managed at the SplitReader level. So we need to create one
        // locally here just to me able to create the mapper.
        final Cluster cluster = clusterBuilder.getCluster();
        final Session session = cluster.connect();
        mapper = new MappingManager(session).mapper(pojoClass);
        if (mapperOptions != null) {
            Mapper.Option[] optionsArray = mapperOptions.getMapperOptions();
            if (optionsArray != null) {
                mapper.setDefaultGetOptions(optionsArray);
            }
        }
        // close the temporary cluster and session
        session.close();
        cluster.close();
    }

    @Override
    public void emitRecord(
            CassandraRow cassandraRow,
            SourceOutput<OUT> output,
            CassandraSplitState cassandraSplitState) {
        final Row row = cassandraRow.getRow();
        // Mapping from a row to a Class<OUT> is a complex operation involving reflection API.
        // It is better to use Cassandra mapper for it.
        // but the mapper takes only a resultSet as input hence forging one containing the row
        ResultSet resultSet =
                new ResultSet() {
                    @Override
                    public Row one() {
                        return row;
                    }

                    @Override
                    public ColumnDefinitions getColumnDefinitions() {
                        return row.getColumnDefinitions();
                    }

                    @Override
                    public boolean wasApplied() {
                        return true;
                    }

                    @Override
                    public boolean isExhausted() {
                        return true;
                    }

                    @Override
                    public boolean isFullyFetched() {
                        return true;
                    }

                    @Override
                    public int getAvailableWithoutFetching() {
                        return 1;
                    }

                    @Override
                    public ListenableFuture<ResultSet> fetchMoreResults() {
                        return Futures.immediateFuture(null);
                    }

                    @Override
                    public List<Row> all() {
                        return Collections.singletonList(row);
                    }

                    @Override
                    public Iterator<Row> iterator() {
                        return new Iterator<Row>() {

                            @Override
                            public boolean hasNext() {
                                return true;
                            }

                            @Override
                            public Row next() {
                                return row;
                            }
                        };
                    }

                    @Override
                    public ExecutionInfo getExecutionInfo() {
                        return cassandraRow.getExecutionInfo();
                    }

                    @Override
                    public List<ExecutionInfo> getAllExecutionInfo() {
                        return Collections.singletonList(cassandraRow.getExecutionInfo());
                    }
                };
        // output the pojo based on the cassandraRow
        output.collect(mapper.map(resultSet).one());
        // update cassandraSplitState to reflect the emitted records
        cassandraSplitState.markRingRangeAsFinished(cassandraRow.getAssociatedRingRange());
    }
}
