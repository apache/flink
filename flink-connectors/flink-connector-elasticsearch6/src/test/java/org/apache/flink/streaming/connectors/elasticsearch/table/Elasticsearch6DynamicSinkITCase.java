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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.testutils.ElasticsearchContainerUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.flink.streaming.connectors.elasticsearch.table.TestContext.context;
import static org.apache.flink.table.api.Expressions.row;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/** IT tests for {@link Elasticsearch6DynamicSink}. */
public class Elasticsearch6DynamicSinkITCase extends TestLogger {
    private static final Logger LOG =
            LoggerFactory.getLogger(Elasticsearch6DynamicSinkITCase.class);

    @ClassRule
    public static ElasticsearchContainer elasticsearchContainer =
            ElasticsearchContainerUtil.createElasticsearchContainer(
                    DockerImageVersions.ELASTICSEARCH_6, LOG);

    @SuppressWarnings("deprecation")
    protected final Client getClient() {
        TransportAddress transportAddress =
                new TransportAddress(elasticsearchContainer.getTcpHost());
        String expectedClusterName = "docker-cluster";
        Settings settings = Settings.builder().put("cluster.name", expectedClusterName).build();
        return new PreBuiltTransportClient(settings).addTransportAddress(transportAddress);
    }

    @Test
    public void testWritingDocuments() throws Exception {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT().notNull()),
                                Column.physical("b", DataTypes.TIME()),
                                Column.physical("c", DataTypes.STRING().notNull()),
                                Column.physical("d", DataTypes.FLOAT()),
                                Column.physical("e", DataTypes.TINYINT().notNull()),
                                Column.physical("f", DataTypes.DATE()),
                                Column.physical("g", DataTypes.TIMESTAMP().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("name", Arrays.asList("a", "g")));
        GenericRowData rowData =
                GenericRowData.of(
                        1L,
                        12345,
                        StringData.fromString("ABCDE"),
                        12.12f,
                        (byte) 2,
                        12345,
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2012-12-12T12:12:12")));

        String index = "writing-documents";
        String myType = "MyType";
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        SinkFunctionProvider sinkRuntimeProvider =
                (SinkFunctionProvider)
                        sinkFactory
                                .createDynamicTableSink(
                                        context()
                                                .withSchema(schema)
                                                .withOption(
                                                        ElasticsearchConnectorOptions.INDEX_OPTION
                                                                .key(),
                                                        index)
                                                .withOption(
                                                        ElasticsearchConnectorOptions
                                                                .DOCUMENT_TYPE_OPTION
                                                                .key(),
                                                        myType)
                                                .withOption(
                                                        ElasticsearchConnectorOptions.HOSTS_OPTION
                                                                .key(),
                                                        elasticsearchContainer.getHttpHostAddress())
                                                .withOption(
                                                        ElasticsearchConnectorOptions
                                                                .FLUSH_ON_CHECKPOINT_OPTION
                                                                .key(),
                                                        "false")
                                                .build())
                                .getSinkRuntimeProvider(new MockContext());

        SinkFunction<RowData> sinkFunction = sinkRuntimeProvider.createSinkFunction();
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);

        rowData.setRowKind(RowKind.UPDATE_AFTER);
        environment.<RowData>fromElements(rowData).addSink(sinkFunction);
        environment.execute();

        Client client = getClient();
        Map<String, Object> response =
                client.get(new GetRequest(index, myType, "1_2012-12-12T12:12:12"))
                        .actionGet()
                        .getSource();
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "00:00:12");
        expectedMap.put("c", "ABCDE");
        expectedMap.put("d", 12.12d);
        expectedMap.put("e", 2);
        expectedMap.put("f", "2003-10-20");
        expectedMap.put("g", "2012-12-12 12:12:12");
        assertThat(response, equalTo(expectedMap));
    }

    @Test
    public void testWritingDocumentsFromTableApi() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "table-api";
        String myType = "MyType";
        tableEnvironment.executeSql(
                "CREATE TABLE esTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIME,\n"
                        + "c STRING NOT NULL,\n"
                        + "d FLOAT,\n"
                        + "e TINYINT NOT NULL,\n"
                        + "f DATE,\n"
                        + "g TIMESTAMP NOT NULL,\n"
                        + "h as a + 2,\n"
                        + "PRIMARY KEY (a, g) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "elasticsearch-6")
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION.key(), myType)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                elasticsearchContainer.getHttpHostAddress())
                        + String.format(
                                "'%s'='%s'\n",
                                ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION.key(),
                                "false")
                        + ")");

        tableEnvironment
                .fromValues(
                        row(
                                1L,
                                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                                "ABCDE",
                                12.12f,
                                (byte) 2,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("esTable")
                .await();

        Client client = getClient();
        Map<String, Object> response =
                client.get(new GetRequest(index, myType, "1_2012-12-12T12:12:12"))
                        .actionGet()
                        .getSource();
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "00:00:12");
        expectedMap.put("c", "ABCDE");
        expectedMap.put("d", 12.12d);
        expectedMap.put("e", 2);
        expectedMap.put("f", "2003-10-20");
        expectedMap.put("g", "2012-12-12 12:12:12");
        assertThat(response, equalTo(expectedMap));
    }

    @Test
    public void testWritingDocumentsNoPrimaryKey() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "no-primary-key";
        String myType = "MyType";
        tableEnvironment.executeSql(
                "CREATE TABLE esTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIME,\n"
                        + "c STRING NOT NULL,\n"
                        + "d FLOAT,\n"
                        + "e TINYINT NOT NULL,\n"
                        + "f DATE,\n"
                        + "g TIMESTAMP NOT NULL\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "elasticsearch-6")
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION.key(), myType)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                elasticsearchContainer.getHttpHostAddress())
                        + String.format(
                                "'%s'='%s'\n",
                                ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION.key(),
                                "false")
                        + ")");

        tableEnvironment
                .fromValues(
                        row(
                                1L,
                                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                                "ABCDE",
                                12.12f,
                                (byte) 2,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2012-12-12T12:12:12")),
                        row(
                                2L,
                                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                                "FGHIJK",
                                13.13f,
                                (byte) 4,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2013-12-12T13:13:13")))
                .executeInsert("esTable")
                .await();

        Client client = getClient();

        // search API does not return documents that were not indexed, we might need to query
        // the index a few times
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
        SearchHits hits;
        do {
            hits = client.prepareSearch(index).execute().actionGet().getHits();
            if (hits.getTotalHits() < 2) {
                Thread.sleep(200);
            }
        } while (hits.getTotalHits() < 2 && deadline.hasTimeLeft());

        if (hits.getTotalHits() < 2) {
            throw new AssertionError("Could not retrieve results from Elasticsearch.");
        }

        HashSet<Map<String, Object>> resultSet = new HashSet<>();
        resultSet.add(hits.getAt(0).getSourceAsMap());
        resultSet.add(hits.getAt(1).getSourceAsMap());
        Map<Object, Object> expectedMap1 = new HashMap<>();
        expectedMap1.put("a", 1);
        expectedMap1.put("b", "00:00:12");
        expectedMap1.put("c", "ABCDE");
        expectedMap1.put("d", 12.12d);
        expectedMap1.put("e", 2);
        expectedMap1.put("f", "2003-10-20");
        expectedMap1.put("g", "2012-12-12 12:12:12");
        Map<Object, Object> expectedMap2 = new HashMap<>();
        expectedMap2.put("a", 2);
        expectedMap2.put("b", "00:00:12");
        expectedMap2.put("c", "FGHIJK");
        expectedMap2.put("d", 13.13d);
        expectedMap2.put("e", 4);
        expectedMap2.put("f", "2003-10-20");
        expectedMap2.put("g", "2013-12-12 13:13:13");
        HashSet<Map<Object, Object>> expectedSet = new HashSet<>();
        expectedSet.add(expectedMap1);
        expectedSet.add(expectedMap2);
        assertThat(resultSet, equalTo(expectedSet));
    }

    @Test
    public void testWritingDocumentsWithDynamicIndex() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "dynamic-index-{b|yyyy-MM-dd}";
        String myType = "MyType";
        tableEnvironment.executeSql(
                "CREATE TABLE esTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIMESTAMP NOT NULL,\n"
                        + "PRIMARY KEY (a) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "elasticsearch-6")
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION.key(), myType)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                elasticsearchContainer.getHttpHostAddress())
                        + String.format(
                                "'%s'='%s'\n",
                                ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION.key(),
                                "false")
                        + ")");

        tableEnvironment
                .fromValues(row(1L, LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("esTable")
                .await();

        Client client = getClient();
        Map<String, Object> response =
                client.get(new GetRequest("dynamic-index-2012-12-12", myType, "1"))
                        .actionGet()
                        .getSource();
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "2012-12-12 12:12:12");
        assertThat(response, equalTo(expectedMap));
    }

    private static class MockContext implements DynamicTableSink.Context {
        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
            return null;
        }

        @Override
        public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                DataType consumedDataType) {
            return null;
        }
    }
}
