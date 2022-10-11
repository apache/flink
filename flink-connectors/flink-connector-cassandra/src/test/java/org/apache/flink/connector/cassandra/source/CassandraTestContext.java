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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.flink.connectors.cassandra.utils.Pojo;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Junit context {@link DataStreamSourceExternalContext} that contains everything related to
 * Cassandra source test cases especially test table management.
 */
public class CassandraTestContext implements DataStreamSourceExternalContext<Pojo> {

    static final String TABLE_NAME = "batches";

    private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE "
                    + CassandraTestEnvironment.KEYSPACE
                    + "."
                    + TABLE_NAME
                    + " (id text PRIMARY KEY, counter int, batch_id int)"
                    + ";";

    private static final String DROP_TABLE_QUERY =
            "DROP TABLE " + CassandraTestEnvironment.KEYSPACE + "." + TABLE_NAME + ";";

    private static final int RECORDS_PER_SPLIT = 20;

    private final Mapper<Pojo> mapper;
    private final MapperOptions mapperOptions;
    private final ClusterBuilder clusterBuilder;
    private final Session session;
    private ExternalSystemSplitDataWriter<Pojo> splitDataWriter;

    public CassandraTestContext(CassandraTestEnvironment cassandraTestEnvironment) {
        clusterBuilder = cassandraTestEnvironment.getClusterBuilder();
        session = cassandraTestEnvironment.getSession();
        createTable();
        mapper = new MappingManager(cassandraTestEnvironment.getSession()).mapper(Pojo.class);
        mapperOptions = () -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)};
    }

    @Override
    public TypeInformation<Pojo> getProducedType() {
        return TypeInformation.of(Pojo.class);
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return Collections.emptyList();
    }

    @Override
    public Source<Pojo, ?, ?> createSource(TestingSourceSettings sourceSettings)
            throws UnsupportedOperationException {

        return new CassandraSource<>(
                clusterBuilder,
                Pojo.class,
                String.format(
                        "SELECT * FROM %s.%s;", CassandraTestEnvironment.KEYSPACE, TABLE_NAME),
                mapperOptions);
    }

    @Override
    public ExternalSystemSplitDataWriter<Pojo> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings) {
        splitDataWriter =
                new ExternalSystemSplitDataWriter<Pojo>() {

                    @Override
                    public void writeRecords(List<Pojo> records) {
                        for (Pojo pojo : records) {
                            mapper.save(pojo, mapperOptions.getMapperOptions());
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        // nothing to do, cluster/session is shared at the CassandraTestEnvironment
                        // level
                    }
                };
        return splitDataWriter;
    }

    @Override
    public List<Pojo> generateTestData(
            TestingSourceSettings sourceSettings, int splitIndex, long seed) {
        List<Pojo> testData = new ArrayList<>(RECORDS_PER_SPLIT);
        // generate RECORDS_PER_SPLIT pojos per split and use splitId as pojo batchIndex so that
        // pojos are
        // considered equal when they belong to the same split as requested in implementation
        // notes.
        for (int i = 0; i < RECORDS_PER_SPLIT; i++) {
            Pojo pojo = new Pojo(String.valueOf(seed + i), i, splitIndex);
            testData.add(pojo);
        }
        return testData;
    }

    @Override
    public void close() throws Exception {
        splitDataWriter.close();
        dropTable();
        // NB: cluster/session is shared at the CassandraTestEnvironment level
    }

    private void createTable() {
        session.execute(CassandraTestEnvironment.requestWithTimeout(CREATE_TABLE_QUERY));
    }

    private void dropTable() {
        session.execute(CassandraTestEnvironment.requestWithTimeout(DROP_TABLE_QUERY));
    }

    static class CassandraTestContextFactory
            implements ExternalContextFactory<CassandraTestContext> {

        private final CassandraTestEnvironment cassandraTestEnvironment;

        public CassandraTestContextFactory(CassandraTestEnvironment cassandraTestEnvironment) {
            this.cassandraTestEnvironment = cassandraTestEnvironment;
        }

        @Override
        public CassandraTestContext createExternalContext(String testName) {
            return new CassandraTestContext(cassandraTestEnvironment);
        }
    }
}
