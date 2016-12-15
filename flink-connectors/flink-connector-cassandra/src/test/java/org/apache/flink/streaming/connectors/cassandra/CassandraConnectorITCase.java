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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import org.apache.cassandra.service.CassandraDaemon;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.operators.WriteAheadSinkTestBase;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.TestEnvironment;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;
import java.util.UUID;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class CassandraConnectorITCase extends WriteAheadSinkTestBase<Tuple3<String, Integer, Integer>, CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>>> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraConnectorITCase.class);
	private static File tmpDir;

	private static final boolean EMBEDDED = true;

	private static EmbeddedCassandraService cassandra;

	private static ClusterBuilder builder = new ClusterBuilder() {
		@Override
		protected Cluster buildCluster(Cluster.Builder builder) {
			return builder
				.addContactPoint("127.0.0.1")
				.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
				.withoutJMXReporting()
				.withoutMetrics().build();
		}
	};

	private static Cluster cluster;
	private static Session session;

	private static final String CREATE_KEYSPACE_QUERY = "CREATE KEYSPACE flink WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};";
	private static final String DROP_KEYSPACE_QUERY = "DROP KEYSPACE flink;";
	private static final String CREATE_TABLE_QUERY = "CREATE TABLE flink.test (id text PRIMARY KEY, counter int, batch_id int);";
	private static final String CLEAR_TABLE_QUERY = "TRUNCATE flink.test;";
	private static final String INSERT_DATA_QUERY = "INSERT INTO flink.test (id, counter, batch_id) VALUES (?, ?, ?)";
	private static final String SELECT_DATA_QUERY = "SELECT * FROM flink.test;";

	private static final ArrayList<Tuple3<String, Integer, Integer>> collection = new ArrayList<>(20);

	static {
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple3<>(UUID.randomUUID().toString(), i, 0));
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

	private static LocalFlinkMiniCluster flinkCluster;

	// ------------------------------------------------------------------------
	//  Cluster Setup (Cassandra & Flink)
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void startCassandra() throws IOException {

		// check if we should run this test, current Cassandra version requires Java >= 1.8
		org.apache.flink.core.testutils.CommonTestUtils.assumeJava8();

		// generate temporary files
		tmpDir = CommonTestUtils.createTempDirectory();
		ClassLoader classLoader = CassandraConnectorITCase.class.getClassLoader();
		File file = new File(classLoader.getResource("cassandra.yaml").getFile());
		File tmp = new File(tmpDir.getAbsolutePath() + File.separator + "cassandra.yaml");
		
		assertTrue(tmp.createNewFile());

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

		try {
			Thread.sleep(1000 * 10);
		} catch (InterruptedException e) { //give cassandra a few seconds to start up
		}

		cluster = builder.getCluster();
		session = cluster.connect();

		session.execute(CREATE_KEYSPACE_QUERY);
		session.execute(CREATE_TABLE_QUERY);
	}

	@BeforeClass
	public static void startFlink() throws Exception {
		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 4);

		flinkCluster = new LocalFlinkMiniCluster(config);
		flinkCluster.start();
	}

	@AfterClass
	public static void stopFlink() {
		if (flinkCluster != null) {
			flinkCluster.stop();
			flinkCluster = null;
		}
	}

	@AfterClass
	public static void closeCassandra() {
		if (session != null) {
			session.executeAsync(DROP_KEYSPACE_QUERY);
			session.close();
		}

		if (cluster != null) {
			cluster.close();
		}

		if (cassandra != null) {
			cassandra.stop();
		}

		if (tmpDir != null) {
			//noinspection ResultOfMethodCallIgnored
			tmpDir.delete();
		}
	}

	// ------------------------------------------------------------------------
	//  Test preparation & cleanup
	// ------------------------------------------------------------------------

	@Before
	public void initializeExecutionEnvironment() {
		TestStreamEnvironment.setAsContext(flinkCluster, 4);
		new TestEnvironment(flinkCluster, 4, false).setAsContext();
	}

	@After
	public void deleteSchema() throws Exception {
		session.executeAsync(CLEAR_TABLE_QUERY);
	}

	// ------------------------------------------------------------------------
	//  Exactly-once Tests
	// ------------------------------------------------------------------------

	@Override
	protected CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> createSink() throws Exception {
		return new CassandraTupleWriteAheadSink<>(
			INSERT_DATA_QUERY,
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

		ResultSet result = session.execute(SELECT_DATA_QUERY);
		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 60; x++) {
			list.add(x);
		}

		for (Row s : result) {
			list.remove(new Integer(s.getInt("counter")));
		}
		Assert.assertTrue("The following ID's were not found in the ResultSet: " + list.toString(), list.isEmpty());
	}

	@Override
	protected void verifyResultsDataPersistenceUponMissedNotify(CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

		ResultSet result = session.execute(SELECT_DATA_QUERY);
		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 60; x++) {
			list.add(x);
		}

		for (Row s : result) {
			list.remove(new Integer(s.getInt("counter")));
		}
		Assert.assertTrue("The following ID's were not found in the ResultSet: " + list.toString(), list.isEmpty());
	}

	@Override
	protected void verifyResultsDataDiscardingUponRestore(CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

		ResultSet result = session.execute(SELECT_DATA_QUERY);
		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 20; x++) {
			list.add(x);
		}
		for (int x = 41; x <= 60; x++) {
			list.add(x);
		}

		for (Row s : result) {
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
		ResultSet result = session.execute(SELECT_DATA_QUERY);
		for (Row s : result) {
			actual.add(s.getInt("counter"));
		}

		Collections.sort(actual);
		Assert.assertArrayEquals(expected.toArray(), actual.toArray());
	}

	@Test
	public void testCassandraCommitter() throws Exception {
		CassandraCommitter cc1 = new CassandraCommitter(builder);
		cc1.setJobId("job");
		cc1.setOperatorId("operator");

		CassandraCommitter cc2 = new CassandraCommitter(builder);
		cc2.setJobId("job");
		cc2.setOperatorId("operator");

		CassandraCommitter cc3 = new CassandraCommitter(builder);
		cc3.setJobId("job");
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

		cc1 = new CassandraCommitter(builder);
		cc1.setJobId("job");
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
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple3<String, Integer, Integer>> source = env.fromCollection(collection);
		source.addSink(new CassandraTupleSink<Tuple3<String, Integer, Integer>>(INSERT_DATA_QUERY, builder));

		env.execute();

		ResultSet rs = session.execute(SELECT_DATA_QUERY);
		Assert.assertEquals(20, rs.all().size());
	}

	@Test
	public void testCassandraPojoAtLeastOnceSink() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<Pojo> source = env
			.addSource(new SourceFunction<Pojo>() {

				private boolean running = true;
				private volatile int cnt = 0;

				@Override
				public void run(SourceContext<Pojo> ctx) throws Exception {
					while (running) {
						ctx.collect(new Pojo(UUID.randomUUID().toString(), cnt, 0));
						cnt++;
						if (cnt == 20) {
							cancel();
						}
					}
				}

				@Override
				public void cancel() {
					running = false;
				}
			});

		source.addSink(new CassandraPojoSink<>(Pojo.class, builder));

		env.execute();

		ResultSet rs = session.execute(SELECT_DATA_QUERY);
		Assert.assertEquals(20, rs.all().size());
	}

	@Test
	public void testCassandraBatchFormats() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataSet<Tuple3<String, Integer, Integer>> dataSet = env.fromCollection(collection);
		dataSet.output(new CassandraOutputFormat<Tuple3<String, Integer, Integer>>(INSERT_DATA_QUERY, builder));

		env.execute("Write data");

		DataSet<Tuple3<String, Integer, Integer>> inputDS = env.createInput(
			new CassandraInputFormat<Tuple3<String, Integer, Integer>>(SELECT_DATA_QUERY, builder),
			TypeInformation.of(new TypeHint<Tuple3<String, Integer, Integer>>(){}));


		long count = inputDS.count();
		Assert.assertEquals(count, 20L);
	}
}
