/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.operators.AtLeastOnceSinkTestBase;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.UUID;

public class CassandraConnectorTest extends AtLeastOnceSinkTestBase<Tuple3<String, Integer, Integer>, CassandraIdempotentExactlyOnceSink<Tuple3<String, Integer, Integer>>> {
	private static File tmpDir;

	private static final boolean EMBEDDED = true;
	private static EmbeddedCassandraService cassandra;
	private transient static ClusterBuilder builder = new ClusterBuilder() {
		@Override
		protected Cluster buildCluster(Cluster.Builder builder) {
			return builder.addContactPoint("127.0.0.1").build();
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
			collection.add(new Tuple3<>("" + UUID.randomUUID(), i, 0));
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

	//=====Setup========================================================================================================
	@BeforeClass
	public static void startCassandra() throws IOException {
		//generate temporary files
		tmpDir = CommonTestUtils.createTempDirectory();
		ClassLoader classLoader = CassandraIdempotentExactlyOnceSink.class.getClassLoader();
		File file = new File(classLoader.getResource("cassandra.yaml").getFile());
		File tmp = new File(tmpDir.getAbsolutePath() + File.separator + "cassandra.yaml");
		tmp.createNewFile();
		BufferedWriter b = new BufferedWriter(new FileWriter(tmp));

		//copy cassandra.yaml; inject absolute paths into cassandra.yaml
		Scanner scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			line = line.replace("$PATH", "'" + tmp.getParentFile());
			b.write(line + "\n");
			b.flush();
		}
		scanner.close();


		// Tell cassandra where the configuration files are.
		// Use the test configuration file.
		System.setProperty("cassandra.config", "file:" + File.separator + File.separator + File.separator + tmp.getAbsolutePath());

		if (EMBEDDED) {
			cassandra = new EmbeddedCassandraService();
			cassandra.start();
		}

		try {
			Thread.sleep(1000 * 10);
		} catch (InterruptedException e) { //give cassandra a few seconds to start up
		}

		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect();

		session.execute(CREATE_KEYSPACE_QUERY);
		session.execute(CREATE_TABLE_QUERY);
	}

	@After
	public void deleteSchema() throws Exception {
		session.executeAsync(CLEAR_TABLE_QUERY);
	}

	@AfterClass
	public static void closeCassandra() {
		session.executeAsync(DROP_KEYSPACE_QUERY);
		session.close();
		cluster.close();
		if (EMBEDDED) {
			cassandra.stop();
		}
		tmpDir.delete();
	}

	//=====Exactly-Once=================================================================================================
	@Override
	protected CassandraIdempotentExactlyOnceSink<Tuple3<String, Integer, Integer>> createSink() {
		try {
			return new CassandraIdempotentExactlyOnceSink<>(
				INSERT_DATA_QUERY,
				TypeExtractor.getForObject(new Tuple3<>("", 0, 0)).createSerializer(new ExecutionConfig()),
				new ClusterBuilder() {
					@Override
					protected Cluster buildCluster(Cluster.Builder builder) {
						return builder.addContactPoint("127.0.0.1").build();
					}
				},
				"testJob",
				new CassandraCommitter(builder));
		} catch (Exception e) {
			Assert.fail("Failure while initializing sink: " + e.getMessage());
			return null;
		}
	}

	@Override
	protected TupleTypeInfo<Tuple3<String, Integer, Integer>> createTypeInfo() {
		return TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Integer.class);
	}

	@Override
	protected Tuple3<String, Integer, Integer> generateValue(int counter, int checkpointID) {
		return new Tuple3<>("" + UUID.randomUUID(), counter, checkpointID);
	}

	@Override
	protected void verifyResultsIdealCircumstances(
		OneInputStreamTaskTestHarness<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> harness,
		OneInputStreamTask<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> task,
		CassandraIdempotentExactlyOnceSink<Tuple3<String, Integer, Integer>> sink) {

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
	protected void verifyResultsDataPersistenceUponMissedNotify(
		OneInputStreamTaskTestHarness<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> harness,
		OneInputStreamTask<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> task,
		CassandraIdempotentExactlyOnceSink<Tuple3<String, Integer, Integer>> sink) {

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
	protected void verifyResultsDataDiscardingUponRestore(
		OneInputStreamTaskTestHarness<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> harness,
		OneInputStreamTask<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> task,
		CassandraIdempotentExactlyOnceSink<Tuple3<String, Integer, Integer>> sink) {

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

	//=====At-Least-Once================================================================================================
	@Test
	public void testCassandraTupleAtLeastOnceSink() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple3<String, Integer, Integer>> source = env.fromCollection(collection);
		source.addSink(new CassandraTupleAtLeastOnceSink<Tuple3<String, Integer, Integer>>(INSERT_DATA_QUERY, builder));

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

		source.addSink(new CassandraPojoAtLeastOnceSink<>(Pojo.class, builder));

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
			new TupleTypeInfo(Tuple3.class, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));


		long count = inputDS.count();
		Assert.assertEquals(count, 20L);
	}
}
