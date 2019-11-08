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
import org.apache.flink.batch.connectors.cassandra.CustomCassandraAnnotatedPojo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.runtime.operators.WriteAheadSinkTestBase;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.types.Row;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import org.apache.cassandra.service.CassandraDaemon;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertTrue;

/**
 * IT cases for all cassandra sinks.
 */
@SuppressWarnings("serial")
@Category(FailsOnJava11.class)
public class CassandraConnectorITCase extends WriteAheadSinkTestBase<Tuple3<String, Integer, Integer>, CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>>> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraConnectorITCase.class);

	private static final boolean EMBEDDED = true;

	private static EmbeddedCassandraService cassandra;

	private static final String HOST = "127.0.0.1";

	private static final int PORT = 9042;

	private static ClusterBuilder builder = new ClusterBuilder() {
		@Override
		protected Cluster buildCluster(Cluster.Builder builder) {
			return builder
				.addContactPointsWithPorts(new InetSocketAddress(HOST, PORT))
				.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
				.withoutJMXReporting()
				.withoutMetrics().build();
		}
	};

	private static Cluster cluster;
	private static Session session;

	private static final String TABLE_NAME_PREFIX = "flink_";
	private static final String TABLE_NAME_VARIABLE = "$TABLE";
	private static final String CREATE_KEYSPACE_QUERY = "CREATE KEYSPACE flink WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};";
	private static final String CREATE_TABLE_QUERY = "CREATE TABLE flink." + TABLE_NAME_VARIABLE + " (id text PRIMARY KEY, counter int, batch_id int);";
	private static final String INSERT_DATA_QUERY = "INSERT INTO flink." + TABLE_NAME_VARIABLE + " (id, counter, batch_id) VALUES (?, ?, ?)";
	private static final String SELECT_DATA_QUERY = "SELECT * FROM flink." + TABLE_NAME_VARIABLE + ';';

	private static final Random random = new Random();
	private int tableID;

	private static final ArrayList<Tuple3<String, Integer, Integer>> collection = new ArrayList<>(20);
	private static final ArrayList<Row> rowCollection = new ArrayList<>(20);

	private static final TypeInformation[] FIELD_TYPES = {
		BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};

	static {
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple3<>(UUID.randomUUID().toString(), i, 0));
			rowCollection.add(Row.of(UUID.randomUUID().toString(), i, 0));
		}
	}

	private static class EmbeddedCassandraService {
		CassandraDaemon cassandraDaemon;

		public void start() throws IOException {
			this.cassandraDaemon = new CassandraDaemon();
			this.cassandraDaemon.init(null);
			this.cassandraDaemon.start();
		}

		public void stop() {
			this.cassandraDaemon.stop();
		}
	}

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@BeforeClass
	public static void startCassandra() throws IOException {

		ClassLoader classLoader = CassandraConnectorITCase.class.getClassLoader();
		File file = new File(classLoader.getResource("cassandra.yaml").getFile());
		File tmp = TEMPORARY_FOLDER.newFile("cassandra.yaml");

		try (
			BufferedWriter b = new BufferedWriter(new FileWriter(tmp));

			//copy cassandra.yaml; inject absolute paths into cassandra.yaml
			Scanner scanner = new Scanner(file);
		) {
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				line = line.replace("$PATH", "'" + tmp.getParentFile());
				b.write(line + "\n");
				b.flush();
			}
		}

		// Tell cassandra where the configuration files are.
		// Use the test configuration file.
		System.setProperty("cassandra.config", tmp.getAbsoluteFile().toURI().toString());

		if (EMBEDDED) {
			cassandra = new EmbeddedCassandraService();
			cassandra.start();
		}

		// start establishing a connection within 30 seconds
		long start = System.nanoTime();
		long deadline = start + 30_000_000_000L;
		while (true) {
			try {
				cluster = builder.getCluster();
				session = cluster.connect();
				break;
			} catch (Exception e) {
				if (System.nanoTime() > deadline) {
					throw e;
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException ignored) {
				}
			}
		}
		LOG.debug("Connection established after {}ms.", System.currentTimeMillis() - start);

		session.execute(CREATE_KEYSPACE_QUERY);
		session.execute(CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + "initial"));
	}

	@Before
	public void createTable() {
		tableID = random.nextInt(Integer.MAX_VALUE);
		session.execute(injectTableName(CREATE_TABLE_QUERY));
	}

	@AfterClass
	public static void closeCassandra() {
		if (session != null) {
			session.close();
		}

		if (cluster != null) {
			cluster.close();
		}

		if (cassandra != null) {
			cassandra.stop();
		}
	}

	// ------------------------------------------------------------------------
	//  Exactly-once Tests
	// ------------------------------------------------------------------------

	@Override
	protected CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> createSink() throws Exception {
		return new CassandraTupleWriteAheadSink<>(
			injectTableName(INSERT_DATA_QUERY),
			TypeExtractor.getForObject(new Tuple3<>("", 0, 0)).createSerializer(new ExecutionConfig()),
			builder,
			new CassandraCommitter(builder));
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
	protected void verifyResultsIdealCircumstances(CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

		ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));
		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 60; x++) {
			list.add(x);
		}

		for (com.datastax.driver.core.Row s : result) {
			list.remove(new Integer(s.getInt("counter")));
		}
		Assert.assertTrue("The following ID's were not found in the ResultSet: " + list.toString(), list.isEmpty());
	}

	@Override
	protected void verifyResultsDataPersistenceUponMissedNotify(CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

		ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));
		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 60; x++) {
			list.add(x);
		}

		for (com.datastax.driver.core.Row s : result) {
			list.remove(new Integer(s.getInt("counter")));
		}
		Assert.assertTrue("The following ID's were not found in the ResultSet: " + list.toString(), list.isEmpty());
	}

	@Override
	protected void verifyResultsDataDiscardingUponRestore(CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

		ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));
		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 20; x++) {
			list.add(x);
		}
		for (int x = 41; x <= 60; x++) {
			list.add(x);
		}

		for (com.datastax.driver.core.Row s : result) {
			list.remove(new Integer(s.getInt("counter")));
		}
		Assert.assertTrue("The following ID's were not found in the ResultSet: " + list.toString(), list.isEmpty());
	}

	@Override
	protected void verifyResultsWhenReScaling(
		CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink, int startElementCounter, int endElementCounter) {

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
			actual.add(s.getInt("counter"));
		}

		Collections.sort(actual);
		Assert.assertArrayEquals(expected.toArray(), actual.toArray());
	}

	@Test
	public void testCassandraCommitter() throws Exception {
		String jobID = new JobID().toString();
		CassandraCommitter cc1 = new CassandraCommitter(builder, "flink_auxiliary_cc");
		cc1.setJobId(jobID);
		cc1.setOperatorId("operator");

		CassandraCommitter cc2 = new CassandraCommitter(builder, "flink_auxiliary_cc");
		cc2.setJobId(jobID);
		cc2.setOperatorId("operator");

		CassandraCommitter cc3 = new CassandraCommitter(builder, "flink_auxiliary_cc");
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
		//verify that other sub-tasks aren't affected
		Assert.assertFalse(cc2.isCheckpointCommitted(1, 1));
		//verify that other tasks aren't affected
		Assert.assertFalse(cc3.isCheckpointCommitted(0, 1));

		Assert.assertFalse(cc1.isCheckpointCommitted(0, 2));

		cc1.close();
		cc2.close();
		cc3.close();

		cc1 = new CassandraCommitter(builder, "flink_auxiliary_cc");
		cc1.setJobId(jobID);
		cc1.setOperatorId("operator");

		cc1.open();

		//verify that checkpoint data is not destroyed within open/close and not reliant on internally cached data
		Assert.assertTrue(cc1.isCheckpointCommitted(0, 1));
		Assert.assertFalse(cc1.isCheckpointCommitted(0, 2));

		cc1.close();
	}

	// ------------------------------------------------------------------------
	//  At-least-once Tests
	// ------------------------------------------------------------------------

	@Test
	public void testCassandraTupleAtLeastOnceSink() throws Exception {
		CassandraTupleSink<Tuple3<String, Integer, Integer>> sink = new CassandraTupleSink<>(injectTableName(INSERT_DATA_QUERY), builder);
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
		CassandraRowSink sink = new CassandraRowSink(FIELD_TYPES.length, injectTableName(INSERT_DATA_QUERY), builder);
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
		session.execute(CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, "test"));

		CassandraPojoSink<Pojo> sink = new CassandraPojoSink<>(Pojo.class, builder);
		try {
			sink.open(new Configuration());
			for (int x = 0; x < 20; x++) {
				sink.send(new Pojo(UUID.randomUUID().toString(), x, 0));
			}
		} finally {
			sink.close();
		}

		ResultSet rs = session.execute(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, "test"));
		Assert.assertEquals(20, rs.all().size());
	}

	@Test
	public void testCassandraPojoNoAnnotatedKeyspaceAtLeastOnceSink() throws Exception {
		session.execute(CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, "testPojoNoAnnotatedKeyspace"));

		CassandraPojoSink<PojoNoAnnotatedKeyspace> sink = new CassandraPojoSink<>(PojoNoAnnotatedKeyspace.class, builder, "flink");
		try {
			sink.open(new Configuration());
			for (int x = 0; x < 20; x++) {
				sink.send(new PojoNoAnnotatedKeyspace(UUID.randomUUID().toString(), x, 0));
			}

		} finally {
			sink.close();
		}
		ResultSet rs = session.execute(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, "testPojoNoAnnotatedKeyspace"));
		Assert.assertEquals(20, rs.all().size());
	}

	@Test
	public void testCassandraTableSink() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStreamSource<Row> source = env.fromCollection(rowCollection);

		tEnv.registerDataStream("testFlinkTable", source);
		tEnv.registerTableSink(
			"cassandraTable",
			new CassandraAppendTableSink(builder, injectTableName(INSERT_DATA_QUERY)).configure(
				new String[]{"f0", "f1", "f2"},
				new TypeInformation[]{Types.STRING, Types.INT, Types.INT}
			));

		tEnv.sqlQuery("select * from testFlinkTable").insertInto("cassandraTable");

		env.execute();
		ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));

		// validate that all input was correctly written to Cassandra
		List<Row> input = new ArrayList<>(rowCollection);
		List<com.datastax.driver.core.Row> output = rs.all();
		for (com.datastax.driver.core.Row o : output) {
			Row cmp = new Row(3);
			cmp.setField(0, o.getString(0));
			cmp.setField(1, o.getInt(2));
			cmp.setField(2, o.getInt(1));
			Assert.assertTrue("Row " + cmp + " was written to Cassandra but not in input.", input.remove(cmp));
		}
		Assert.assertTrue("The input data was not completely written to Cassandra", input.isEmpty());
	}

	@Test
	public void testCassandraBatchPojoFormat() throws Exception {

		session.execute(CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, CustomCassandraAnnotatedPojo.TABLE_NAME));

		OutputFormat<CustomCassandraAnnotatedPojo> sink = new CassandraPojoOutputFormat<>(builder, CustomCassandraAnnotatedPojo.class, () -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)});

		List<CustomCassandraAnnotatedPojo> customCassandraAnnotatedPojos = IntStream.range(0, 20)
			.mapToObj(x -> new CustomCassandraAnnotatedPojo(UUID.randomUUID().toString(), x, 0))
			.collect(Collectors.toList());
		try {
			sink.configure(new Configuration());
			sink.open(0, 1);
			for (CustomCassandraAnnotatedPojo customCassandraAnnotatedPojo : customCassandraAnnotatedPojos) {
				sink.writeRecord(customCassandraAnnotatedPojo);
			}
		} finally {
			sink.close();
		}
		ResultSet rs = session.execute(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, CustomCassandraAnnotatedPojo.TABLE_NAME));
		Assert.assertEquals(20, rs.all().size());

		InputFormat<CustomCassandraAnnotatedPojo, InputSplit> source = new CassandraPojoInputFormat<>(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, "batches"), builder, CustomCassandraAnnotatedPojo.class);
		List<CustomCassandraAnnotatedPojo> result = new ArrayList<>();

		try {
			source.configure(new Configuration());
			source.open(null);
			while (!source.reachedEnd()) {
				CustomCassandraAnnotatedPojo temp = source.nextRecord(null);
				result.add(temp);
			}
		} finally {
			source.close();
		}

		Assert.assertEquals(20, result.size());
		result.sort(Comparator.comparingInt(CustomCassandraAnnotatedPojo::getCounter));
		customCassandraAnnotatedPojos.sort(Comparator.comparingInt(CustomCassandraAnnotatedPojo::getCounter));

		assertThat(result, samePropertyValuesAs(customCassandraAnnotatedPojos));
	}

	@Test
	public void testCassandraBatchTupleFormat() throws Exception {
		OutputFormat<Tuple3<String, Integer, Integer>> sink = new CassandraOutputFormat<>(injectTableName(INSERT_DATA_QUERY), builder);
		try {
			sink.configure(new Configuration());
			sink.open(0, 1);
			for (Tuple3<String, Integer, Integer> value : collection) {
				sink.writeRecord(value);
			}
		} finally {
			sink.close();
		}

		sink = new CassandraTupleOutputFormat<>(injectTableName(INSERT_DATA_QUERY), builder);
		try {
			sink.configure(new Configuration());
			sink.open(0, 1);
			for (Tuple3<String, Integer, Integer> value : collection) {
				sink.writeRecord(value);
			}
		} finally {
			sink.close();
		}

		InputFormat<Tuple3<String, Integer, Integer>, InputSplit> source = new CassandraInputFormat<>(injectTableName(SELECT_DATA_QUERY), builder);
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
		OutputFormat<Row> sink = new CassandraRowOutputFormat(injectTableName(INSERT_DATA_QUERY), builder);
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

	private String injectTableName(String target) {
		return target.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + tableID);
	}

	@Test
	public void testCassandraScalaTupleAtLeastOnceSinkBuilderDetection() throws Exception {
		Class<scala.Tuple1<String>> c = (Class<scala.Tuple1<String>>) new scala.Tuple1<>("hello").getClass();
		Seq<TypeInformation<?>> typeInfos = JavaConverters.asScalaBufferConverter(
			Collections.<TypeInformation<?>>singletonList(BasicTypeInfo.STRING_TYPE_INFO)).asScala();
		Seq<String> fieldNames = JavaConverters.asScalaBufferConverter(
			Collections.singletonList("_1")).asScala();

		CaseClassTypeInfo<scala.Tuple1<String>> typeInfo = new CaseClassTypeInfo<scala.Tuple1<String>>(c, null, typeInfos, fieldNames) {
			@Override
			public TypeSerializer<scala.Tuple1<String>> createSerializer(ExecutionConfig config) {
				return null;
			}
		};

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<scala.Tuple1<String>> input = env.fromElements(new scala.Tuple1<>("hello")).returns(typeInfo);

		CassandraSink.CassandraSinkBuilder<scala.Tuple1<String>> sinkBuilder = CassandraSink.addSink(input);
		assertTrue(sinkBuilder instanceof CassandraSink.CassandraScalaProductSinkBuilder);
	}

	@Test
	public void testCassandraScalaTupleAtLeastSink() throws Exception {
		CassandraScalaProductSink<scala.Tuple3<String, Integer, Integer>> sink = new CassandraScalaProductSink<>(injectTableName(INSERT_DATA_QUERY), builder);

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
			scalaTupleCollection.remove(new scala.Tuple3<>(row.getString("id"), row.getInt("counter"), row.getInt("batch_id")));
		}
		Assert.assertEquals(0, scalaTupleCollection.size());
	}

	@Test
	public void testCassandraScalaTuplePartialColumnUpdate() throws Exception {
		CassandraSinkBaseConfig config = CassandraSinkBaseConfig.newBuilder().setIgnoreNullFields(true).build();
		CassandraScalaProductSink<scala.Tuple3<String, Integer, Integer>> sink = new CassandraScalaProductSink<>(injectTableName(INSERT_DATA_QUERY), builder, config);

		String id = UUID.randomUUID().toString();
		Integer counter = 1;
		Integer batchId = 0;

		// Send partial records across multiple request
		scala.Tuple3<String, Integer, Integer> scalaTupleRecordFirst = new scala.Tuple3<>(id, counter, null);
		scala.Tuple3<String, Integer, Integer> scalaTupleRecordSecond = new scala.Tuple3<>(id, null, batchId);

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
			Assert.assertEquals(new scala.Tuple3<>(id, counter, batchId), new scala.Tuple3<>(row.getString("id"), row.getInt("counter"), row.getInt("batch_id")));
		}
	}
}
