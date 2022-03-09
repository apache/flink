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

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraRowOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.runtime.operators.WriteAheadSinkTestBase;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.testutils.junit.RetryOnException;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.types.Row;
import org.apache.flink.util.DockerImageVersions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.Table;
import net.bytebuddy.ByteBuddy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertTrue;

/** IT cases for all cassandra sinks. */
@SuppressWarnings("serial")
// NoHostAvailableException is raised by Cassandra client under load while connecting to the cluster
@RetryOnException(times = 3, exception = NoHostAvailableException.class)
public class CassandraConnectorITCase
        extends WriteAheadSinkTestBase<
                Tuple3<String, Integer, Integer>,
                CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>>> {

    private static final int MAX_CONNECTION_RETRY = 3;
    private static final long CONNECTION_RETRY_DELAY = 500L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraConnectorITCase.class);
    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @ClassRule
    public static final CassandraContainer CASSANDRA_CONTAINER = createCassandraContainer();

    @Rule public final RetryRule retryRule = new RetryRule();

    private static final int PORT = 9042;

    private static Cluster cluster;
    private static Session session;

    private final ClusterBuilder builderForReading =
            createBuilderWithConsistencyLevel(ConsistencyLevel.ONE);
    // Lower consistency level ANY is only available for writing.
    private final ClusterBuilder builderForWriting =
            createBuilderWithConsistencyLevel(ConsistencyLevel.ANY);

    private ClusterBuilder createBuilderWithConsistencyLevel(ConsistencyLevel consistencyLevel) {
        return new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPointsWithPorts(
                                new InetSocketAddress(
                                        CASSANDRA_CONTAINER.getHost(),
                                        CASSANDRA_CONTAINER.getMappedPort(PORT)))
                        .withQueryOptions(
                                new QueryOptions()
                                        .setConsistencyLevel(consistencyLevel)
                                        .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
                        .withSocketOptions(
                                new SocketOptions()
                                        // multiply default timeout by 3
                                        .setConnectTimeoutMillis(15000)
                                        // double default timeout
                                        .setReadTimeoutMillis(24000))
                        .withoutJMXReporting()
                        .withoutMetrics()
                        .build();
            }
        };
    }

    private static final String TABLE_NAME_PREFIX = "flink_";
    private static final String TABLE_NAME_VARIABLE = "$TABLE";
    private static final String KEYSPACE = "flink";
    private static final String TUPLE_ID_FIELD = "id";
    private static final String TUPLE_COUNTER_FIELD = "counter";
    private static final String TUPLE_BATCHID_FIELD = "batch_id";
    private static final String CREATE_KEYSPACE_QUERY =
            "CREATE KEYSPACE "
                    + KEYSPACE
                    + " WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};";
    private static final String DROP_KEYSPACE_QUERY = "DROP KEYSPACE IF EXISTS " + KEYSPACE + " ;";
    private static final String DROP_TABLE_QUERY =
            "DROP TABLE IF EXISTS " + KEYSPACE + "." + TABLE_NAME_VARIABLE + " ;";
    private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE "
                    + KEYSPACE
                    + "."
                    + TABLE_NAME_VARIABLE
                    + " ("
                    + TUPLE_ID_FIELD
                    + " text PRIMARY KEY, "
                    + TUPLE_COUNTER_FIELD
                    + " int, "
                    + TUPLE_BATCHID_FIELD
                    + " int);";
    private static final String INSERT_DATA_QUERY =
            "INSERT INTO "
                    + KEYSPACE
                    + "."
                    + TABLE_NAME_VARIABLE
                    + " ("
                    + TUPLE_ID_FIELD
                    + ", "
                    + TUPLE_COUNTER_FIELD
                    + ", "
                    + TUPLE_BATCHID_FIELD
                    + ") VALUES (?, ?, ?)";
    private static final String SELECT_DATA_QUERY =
            "SELECT * FROM " + KEYSPACE + "." + TABLE_NAME_VARIABLE + ';';

    private static final Random random = new Random();
    private int tableID;

    private static final ArrayList<Tuple3<String, Integer, Integer>> collection =
            new ArrayList<>(20);
    private static final ArrayList<Row> rowCollection = new ArrayList<>(20);

    private static final TypeInformation[] FIELD_TYPES = {
        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
    };

    static {
        for (int i = 0; i < 20; i++) {
            collection.add(new Tuple3<>(UUID.randomUUID().toString(), i, 0));
            rowCollection.add(Row.of(UUID.randomUUID().toString(), i, 0));
        }
    }

    private static Class<? extends Pojo> annotatePojoWithTable(String keyspace, String tableName) {
        return new ByteBuddy()
                .redefine(Pojo.class)
                .name("org.apache.flink.streaming.connectors.cassandra.Pojo" + tableName)
                .annotateType(createTableAnnotation(keyspace, tableName))
                .make()
                .load(Pojo.class.getClassLoader())
                .getLoaded();
    }

    @NotNull
    private static Table createTableAnnotation(String keyspace, String tableName) {
        return new Table() {

            @Override
            public String keyspace() {
                return keyspace;
            }

            @Override
            public String name() {
                return tableName;
            }

            @Override
            public boolean caseSensitiveKeyspace() {
                return false;
            }

            @Override
            public boolean caseSensitiveTable() {
                return false;
            }

            @Override
            public String writeConsistency() {
                return "";
            }

            @Override
            public String readConsistency() {
                return "";
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return Table.class;
            }
        };
    }

    // ------------------------------------------------------------------------
    //  Utility methods
    // ------------------------------------------------------------------------

    public static CassandraContainer createCassandraContainer() {
        CassandraContainer cassandra = new CassandraContainer(DockerImageVersions.CASSANDRA_3);
        cassandra.withJmxReporting(false);
        cassandra.withLogConsumer(LOG_CONSUMER);
        return cassandra;
    }

    private static void raiseCassandraRequestsTimeouts() {
        try {
            final Path configurationPath = TEMPORARY_FOLDER.newFile().toPath();
            CASSANDRA_CONTAINER.copyFileFromContainer(
                    "/etc/cassandra/cassandra.yaml", configurationPath.toAbsolutePath().toString());
            String configuration =
                    new String(Files.readAllBytes(configurationPath), StandardCharsets.UTF_8);
            String patchedConfiguration =
                    configuration
                            .replaceAll(
                                    "request_timeout_in_ms: [0-9]+", "request_timeout_in_ms: 30000")
                            .replaceAll(
                                    "read_request_timeout_in_ms: [0-9]+",
                                    "read_request_timeout_in_ms: 15000")
                            .replaceAll(
                                    "write_request_timeout_in_ms: [0-9]+",
                                    "write_request_timeout_in_ms: 6000");
            CASSANDRA_CONTAINER.copyFileToContainer(
                    Transferable.of(patchedConfiguration.getBytes(StandardCharsets.UTF_8)),
                    "/etc/cassandra/cassandra.yaml");
        } catch (IOException e) {
            throw new RuntimeException("Unable to open Cassandra configuration file ", e);
        }
    }

    private <T> List<T> readPojosWithInputFormat(Class<T> annotatedPojoClass) {
        final CassandraPojoInputFormat<T> source =
                new CassandraPojoInputFormat<>(
                        injectTableName(SELECT_DATA_QUERY), builderForReading, annotatedPojoClass);
        List<T> result = new ArrayList<>();

        try {
            source.configure(new Configuration());
            source.open(null);
            while (!source.reachedEnd()) {
                T temp = source.nextRecord(null);
                result.add(temp);
            }
        } finally {
            source.close();
        }
        return result;
    }

    private <T> List<T> writePojosWithOutputFormat(Class<T> annotatedPojoClass) throws Exception {
        final CassandraPojoOutputFormat<T> sink =
                new CassandraPojoOutputFormat<>(
                        builderForWriting,
                        annotatedPojoClass,
                        () -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)});

        final Constructor<T> pojoConstructor = getPojoConstructor(annotatedPojoClass);
        List<T> pojos = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            pojos.add(pojoConstructor.newInstance(UUID.randomUUID().toString(), i, 0));
        }
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (T pojo : pojos) {
                sink.writeRecord(pojo);
            }
        } finally {
            sink.close();
        }
        return pojos;
    }

    private <T> Constructor<T> getPojoConstructor(Class<T> annotatedPojoClass)
            throws NoSuchMethodException {
        return annotatedPojoClass.getConstructor(String.class, Integer.TYPE, Integer.TYPE);
    }

    private String injectTableName(String target) {
        return target.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + tableID);
    }

    // ------------------------------------------------------------------------
    //  Tests initialization
    // ------------------------------------------------------------------------

    @BeforeClass
    public static void startAndInitializeCassandra() {
        raiseCassandraRequestsTimeouts();
        // CASSANDRA_CONTAINER#start() already contains retrials
        CASSANDRA_CONTAINER.start();
        cluster = CASSANDRA_CONTAINER.getCluster();
        int retried = 0;
        while (retried < MAX_CONNECTION_RETRY) {
            try {
                session = cluster.connect();
                break;
            } catch (NoHostAvailableException e) {
                retried++;
                LOG.debug(
                        "Connection failed with NoHostAvailableException : retry number {}, will retry to connect within {} ms",
                        retried,
                        CONNECTION_RETRY_DELAY);
                if (retried == MAX_CONNECTION_RETRY) {
                    throw new RuntimeException(
                            String.format(
                                    "Failed to connect to Cassandra cluster after %d retries every %d ms",
                                    retried, CONNECTION_RETRY_DELAY),
                            e);
                }
                try {
                    Thread.sleep(CONNECTION_RETRY_DELAY);
                } catch (InterruptedException ignored) {
                }
            }
        }
        session.execute(DROP_KEYSPACE_QUERY);
        session.execute(CREATE_KEYSPACE_QUERY);
        session.execute(
                CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + "initial"));
    }

    @Before
    public void createTable() {
        tableID = random.nextInt(Integer.MAX_VALUE);
        session.execute(injectTableName(CREATE_TABLE_QUERY));
    }

    @After
    public void dropTables() {
        // need to drop tables in case of retrials. Need to drop all the tables
        // that are created in test because this method is executed with every test
        session.execute(DROP_KEYSPACE_QUERY);
        session.execute(CREATE_KEYSPACE_QUERY);
    }

    @AfterClass
    public static void closeCassandra() {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        CASSANDRA_CONTAINER.stop();
    }

    // ------------------------------------------------------------------------
    //  Technical Tests
    // ------------------------------------------------------------------------

    @Test
    public void testAnnotatePojoWithTable() {
        final String tableName = TABLE_NAME_PREFIX + tableID;

        final Class<? extends Pojo> annotatedPojoClass = annotatePojoWithTable(KEYSPACE, tableName);
        final Table pojoTableAnnotation = annotatedPojoClass.getAnnotation(Table.class);
        assertTrue(pojoTableAnnotation.name().contains(tableName));
    }

    @Test
    public void testRaiseCassandraRequestsTimeouts() throws IOException {
        // raiseCassandraRequestsTimeouts() was already called in @BeforeClass,
        // do not change the container conf twice, just assert that it was indeed changed in the
        // container
        final Path configurationPath = TEMPORARY_FOLDER.newFile().toPath();
        CASSANDRA_CONTAINER.copyFileFromContainer(
                "/etc/cassandra/cassandra.yaml", configurationPath.toAbsolutePath().toString());
        final String configuration =
                new String(Files.readAllBytes(configurationPath), StandardCharsets.UTF_8);
        assertTrue(configuration.contains("request_timeout_in_ms: 30000"));
        assertTrue(configuration.contains("read_request_timeout_in_ms: 15000"));
        assertTrue(configuration.contains("write_request_timeout_in_ms: 6000"));
    }

    // ------------------------------------------------------------------------
    //  Exactly-once Tests
    // ------------------------------------------------------------------------

    @Override
    protected CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> createSink()
            throws Exception {
        return new CassandraTupleWriteAheadSink<>(
                injectTableName(INSERT_DATA_QUERY),
                TypeExtractor.getForObject(new Tuple3<>("", 0, 0))
                        .createSerializer(new ExecutionConfig()),
                builderForReading,
                new CassandraCommitter(builderForReading));
    }

    @Override
    protected TupleTypeInfo<Tuple3<String, Integer, Integer>> createTypeInfo() {
        return TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Integer.class);
    }

    @Override
    protected Tuple3<String, Integer, Integer> generateValue(int counter, int checkpointID) {
        return new Tuple3<>(UUID.randomUUID().toString(), counter, checkpointID);
    }

    @Override
    protected void verifyResultsIdealCircumstances(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

        ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 1; x <= 60; x++) {
            list.add(x);
        }

        for (com.datastax.driver.core.Row s : result) {
            list.remove(new Integer(s.getInt(TUPLE_COUNTER_FIELD)));
        }
        Assert.assertTrue(
                "The following ID's were not found in the ResultSet: " + list.toString(),
                list.isEmpty());
    }

    @Override
    protected void verifyResultsDataPersistenceUponMissedNotify(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

        ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 1; x <= 60; x++) {
            list.add(x);
        }

        for (com.datastax.driver.core.Row s : result) {
            list.remove(new Integer(s.getInt(TUPLE_COUNTER_FIELD)));
        }
        Assert.assertTrue(
                "The following ID's were not found in the ResultSet: " + list.toString(),
                list.isEmpty());
    }

    @Override
    protected void verifyResultsDataDiscardingUponRestore(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

        ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 1; x <= 20; x++) {
            list.add(x);
        }
        for (int x = 41; x <= 60; x++) {
            list.add(x);
        }

        for (com.datastax.driver.core.Row s : result) {
            list.remove(new Integer(s.getInt(TUPLE_COUNTER_FIELD)));
        }
        Assert.assertTrue(
                "The following ID's were not found in the ResultSet: " + list.toString(),
                list.isEmpty());
    }

    @Override
    protected void verifyResultsWhenReScaling(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink,
            int startElementCounter,
            int endElementCounter) {

        // IMPORTANT NOTE:
        //
        // for cassandra we always have to start from 1 because
        // all operators will share the same final db

        ArrayList<Integer> expected = new ArrayList<>();
        for (int i = 1; i <= endElementCounter; i++) {
            expected.add(i);
        }

        ArrayList<Integer> actual = new ArrayList<>();
        ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));

        for (com.datastax.driver.core.Row s : result) {
            actual.add(s.getInt(TUPLE_COUNTER_FIELD));
        }

        Collections.sort(actual);
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void testCassandraCommitter() throws Exception {
        String jobID = new JobID().toString();
        CassandraCommitter cc1 = new CassandraCommitter(builderForReading, "flink_auxiliary_cc");
        cc1.setJobId(jobID);
        cc1.setOperatorId("operator");

        CassandraCommitter cc2 = new CassandraCommitter(builderForReading, "flink_auxiliary_cc");
        cc2.setJobId(jobID);
        cc2.setOperatorId("operator");

        CassandraCommitter cc3 = new CassandraCommitter(builderForReading, "flink_auxiliary_cc");
        cc3.setJobId(jobID);
        cc3.setOperatorId("operator1");

        cc1.createResource();

        cc1.open();
        cc2.open();
        cc3.open();

        Assert.assertFalse(cc1.isCheckpointCommitted(0, 1));
        Assert.assertFalse(cc2.isCheckpointCommitted(1, 1));
        Assert.assertFalse(cc3.isCheckpointCommitted(0, 1));

        cc1.commitCheckpoint(0, 1);
        Assert.assertTrue(cc1.isCheckpointCommitted(0, 1));
        // verify that other sub-tasks aren't affected
        Assert.assertFalse(cc2.isCheckpointCommitted(1, 1));
        // verify that other tasks aren't affected
        Assert.assertFalse(cc3.isCheckpointCommitted(0, 1));

        Assert.assertFalse(cc1.isCheckpointCommitted(0, 2));

        cc1.close();
        cc2.close();
        cc3.close();

        cc1 = new CassandraCommitter(builderForReading, "flink_auxiliary_cc");
        cc1.setJobId(jobID);
        cc1.setOperatorId("operator");

        cc1.open();

        // verify that checkpoint data is not destroyed within open/close and not reliant on
        // internally cached data
        Assert.assertTrue(cc1.isCheckpointCommitted(0, 1));
        Assert.assertFalse(cc1.isCheckpointCommitted(0, 2));

        cc1.close();
    }

    // ------------------------------------------------------------------------
    //  At-least-once Tests
    // ------------------------------------------------------------------------

    @Test
    public void testCassandraTupleAtLeastOnceSink() throws Exception {
        CassandraTupleSink<Tuple3<String, Integer, Integer>> sink =
                new CassandraTupleSink<>(injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.open(new Configuration());
            for (Tuple3<String, Integer, Integer> value : collection) {
                sink.send(value);
            }
        } finally {
            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        Assert.assertEquals(20, rs.all().size());
    }

    @Test
    public void testCassandraRowAtLeastOnceSink() throws Exception {
        CassandraRowSink sink =
                new CassandraRowSink(
                        FIELD_TYPES.length, injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.open(new Configuration());
            for (Row value : rowCollection) {
                sink.send(value);
            }
        } finally {
            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        Assert.assertEquals(20, rs.all().size());
    }

    @Test
    public void testCassandraPojoAtLeastOnceSink() throws Exception {
        final Class<? extends Pojo> annotatedPojoClass =
                annotatePojoWithTable(KEYSPACE, TABLE_NAME_PREFIX + tableID);
        writePojos(annotatedPojoClass, null);

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        Assert.assertEquals(20, rs.all().size());
    }

    @Test
    public void testCassandraPojoNoAnnotatedKeyspaceAtLeastOnceSink() throws Exception {
        final Class<? extends Pojo> annotatedPojoClass =
                annotatePojoWithTable("", TABLE_NAME_PREFIX + tableID);
        writePojos(annotatedPojoClass, KEYSPACE);
        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        Assert.assertEquals(20, rs.all().size());
    }

    private <T> void writePojos(Class<T> annotatedPojoClass, @Nullable String keyspace)
            throws Exception {
        final Constructor<T> pojoConstructor = getPojoConstructor(annotatedPojoClass);
        CassandraPojoSink<T> sink =
                new CassandraPojoSink<>(annotatedPojoClass, builderForWriting, null, keyspace);
        try {
            sink.open(new Configuration());
            for (int x = 0; x < 20; x++) {
                sink.send(pojoConstructor.newInstance(UUID.randomUUID().toString(), x, 0));
            }
        } finally {
            sink.close();
        }
    }

    @Test
    public void testCassandraTableSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Row> source = env.fromCollection(rowCollection);

        tEnv.createTemporaryView("testFlinkTable", source);
        ((TableEnvironmentInternal) tEnv)
                .registerTableSinkInternal(
                        "cassandraTable",
                        new CassandraAppendTableSink(
                                        builderForWriting, injectTableName(INSERT_DATA_QUERY))
                                .configure(
                                        new String[] {"f0", "f1", "f2"},
                                        new TypeInformation[] {
                                            Types.STRING, Types.INT, Types.INT
                                        }));

        tEnv.sqlQuery("select * from testFlinkTable").executeInsert("cassandraTable").await();

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));

        // validate that all input was correctly written to Cassandra
        List<Row> input = new ArrayList<>(rowCollection);
        List<com.datastax.driver.core.Row> output = rs.all();
        for (com.datastax.driver.core.Row o : output) {
            Row cmp = new Row(3);
            cmp.setField(0, o.getString(0));
            cmp.setField(1, o.getInt(2));
            cmp.setField(2, o.getInt(1));
            Assert.assertTrue(
                    "Row " + cmp + " was written to Cassandra but not in input.",
                    input.remove(cmp));
        }
        Assert.assertTrue(
                "The input data was not completely written to Cassandra", input.isEmpty());
    }

    private static int retrialsCount = 0;

    @Test
    public void testRetrialAndDropTables() {
        // should not fail with table exists upon retrial
        // as @After method that truncate the keyspace is called upon retrials.
        annotatePojoWithTable(KEYSPACE, TABLE_NAME_PREFIX + tableID);
        if (retrialsCount < 2) {
            retrialsCount++;
            throw new NoHostAvailableException(new HashMap<>());
        }
    }

    @Test
    public void testCassandraBatchPojoFormat() throws Exception {

        final Class<? extends Pojo> annotatedPojoClass =
                annotatePojoWithTable(KEYSPACE, TABLE_NAME_PREFIX + tableID);

        final List<? extends Pojo> pojos = writePojosWithOutputFormat(annotatedPojoClass);
        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        Assert.assertEquals(20, rs.all().size());

        final List<? extends Pojo> result = readPojosWithInputFormat(annotatedPojoClass);
        Assert.assertEquals(20, result.size());
        assertThat(result, samePropertyValuesAs(pojos));
    }

    @Test
    public void testCassandraBatchTupleFormat() throws Exception {
        OutputFormat<Tuple3<String, Integer, Integer>> sink =
                new CassandraOutputFormat<>(injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (Tuple3<String, Integer, Integer> value : collection) {
                sink.writeRecord(value);
            }
        } finally {
            sink.close();
        }

        sink =
                new CassandraTupleOutputFormat<>(
                        injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (Tuple3<String, Integer, Integer> value : collection) {
                sink.writeRecord(value);
            }
        } finally {
            sink.close();
        }

        InputFormat<Tuple3<String, Integer, Integer>, InputSplit> source =
                new CassandraInputFormat<>(injectTableName(SELECT_DATA_QUERY), builderForReading);
        List<Tuple3<String, Integer, Integer>> result = new ArrayList<>();
        try {
            source.configure(new Configuration());
            source.open(null);
            while (!source.reachedEnd()) {
                result.add(source.nextRecord(new Tuple3<String, Integer, Integer>()));
            }
        } finally {
            source.close();
        }

        Assert.assertEquals(20, result.size());
    }

    @Test
    public void testCassandraBatchRowFormat() throws Exception {
        OutputFormat<Row> sink =
                new CassandraRowOutputFormat(injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (Row value : rowCollection) {
                sink.writeRecord(value);
            }
        } finally {

            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        List<com.datastax.driver.core.Row> rows = rs.all();
        Assert.assertEquals(rowCollection.size(), rows.size());
    }

    @Test
    public void testCassandraScalaTupleAtLeastOnceSinkBuilderDetection() throws Exception {
        Class<scala.Tuple1<String>> c =
                (Class<scala.Tuple1<String>>) new scala.Tuple1<>("hello").getClass();
        Seq<TypeInformation<?>> typeInfos =
                JavaConverters.asScalaBufferConverter(
                                Collections.<TypeInformation<?>>singletonList(
                                        BasicTypeInfo.STRING_TYPE_INFO))
                        .asScala();
        Seq<String> fieldNames =
                JavaConverters.asScalaBufferConverter(Collections.singletonList("_1")).asScala();

        CaseClassTypeInfo<scala.Tuple1<String>> typeInfo =
                new CaseClassTypeInfo<scala.Tuple1<String>>(c, null, typeInfos, fieldNames) {
                    @Override
                    public TypeSerializer<scala.Tuple1<String>> createSerializer(
                            ExecutionConfig config) {
                        return null;
                    }
                };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<scala.Tuple1<String>> input =
                env.fromElements(new scala.Tuple1<>("hello")).returns(typeInfo);

        CassandraSink.CassandraSinkBuilder<scala.Tuple1<String>> sinkBuilder =
                CassandraSink.addSink(input);
        assertTrue(sinkBuilder instanceof CassandraSink.CassandraScalaProductSinkBuilder);
    }

    @Test
    public void testCassandraScalaTupleAtLeastSink() throws Exception {
        CassandraScalaProductSink<scala.Tuple3<String, Integer, Integer>> sink =
                new CassandraScalaProductSink<>(
                        injectTableName(INSERT_DATA_QUERY), builderForWriting);

        List<scala.Tuple3<String, Integer, Integer>> scalaTupleCollection = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            scalaTupleCollection.add(new scala.Tuple3<>(UUID.randomUUID().toString(), i, 0));
        }
        try {
            sink.open(new Configuration());
            for (scala.Tuple3<String, Integer, Integer> value : scalaTupleCollection) {
                sink.invoke(value, SinkContextUtil.forTimestamp(0));
            }
        } finally {
            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        List<com.datastax.driver.core.Row> rows = rs.all();
        Assert.assertEquals(scalaTupleCollection.size(), rows.size());

        for (com.datastax.driver.core.Row row : rows) {
            scalaTupleCollection.remove(
                    new scala.Tuple3<>(
                            row.getString(TUPLE_ID_FIELD),
                            row.getInt(TUPLE_COUNTER_FIELD),
                            row.getInt(TUPLE_BATCHID_FIELD)));
        }
        Assert.assertEquals(0, scalaTupleCollection.size());
    }

    @Test
    public void testCassandraScalaTuplePartialColumnUpdate() throws Exception {
        CassandraSinkBaseConfig config =
                CassandraSinkBaseConfig.newBuilder().setIgnoreNullFields(true).build();
        CassandraScalaProductSink<scala.Tuple3<String, Integer, Integer>> sink =
                new CassandraScalaProductSink<>(
                        injectTableName(INSERT_DATA_QUERY), builderForWriting, config);

        String id = UUID.randomUUID().toString();
        Integer counter = 1;
        Integer batchId = 0;

        // Send partial records across multiple request
        scala.Tuple3<String, Integer, Integer> scalaTupleRecordFirst =
                new scala.Tuple3<>(id, counter, null);
        scala.Tuple3<String, Integer, Integer> scalaTupleRecordSecond =
                new scala.Tuple3<>(id, null, batchId);

        try {
            sink.open(new Configuration());
            sink.invoke(scalaTupleRecordFirst, SinkContextUtil.forTimestamp(0));
            sink.invoke(scalaTupleRecordSecond, SinkContextUtil.forTimestamp(0));
        } finally {
            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        List<com.datastax.driver.core.Row> rows = rs.all();
        Assert.assertEquals(1, rows.size());
        // Since nulls are ignored, we should be reading one complete record
        for (com.datastax.driver.core.Row row : rows) {
            Assert.assertEquals(
                    new scala.Tuple3<>(id, counter, batchId),
                    new scala.Tuple3<>(
                            row.getString(TUPLE_ID_FIELD),
                            row.getInt(TUPLE_COUNTER_FIELD),
                            row.getInt(TUPLE_BATCHID_FIELD)));
        }
    }
}
