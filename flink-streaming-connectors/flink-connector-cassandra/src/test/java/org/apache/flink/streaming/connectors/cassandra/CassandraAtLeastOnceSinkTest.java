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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.runtime.operators.AtLeastOnceSinkTestBase;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.UUID;

public class CassandraAtLeastOnceSinkTest extends AtLeastOnceSinkTestBase<Tuple3<String, Integer, Integer>, CassandraAtLeastOnceSink<Tuple3<String, Integer, Integer>>> {
	private static File tmpDir;

	private static final boolean EMBEDDED = true;
	private static EmbeddedCassandraService cassandra;
	private static Cluster cluster;
	private static Session session;

	private static final String CREATE_KEYSPACE_QUERY = "CREATE KEYSPACE flink WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};";
	private static final String DROP_KEYSPACE_QUERY = "DROP KEYSPACE flink;";
	private static final String CREATE_TABLE_QUERY = "CREATE TABLE flink.test (id text PRIMARY KEY, counter int, batch_id int);";
	private static final String CLEAR_TABLE_QUERY = "TRUNCATE flink.test;";
	private static final String INSERT_DATA_QUERY = "INSERT INTO flink.test (id, counter, batch_id) VALUES (?, ?, ?)";
	private static final String SELECT_DATA_QUERY = "SELECT * FROM flink.test;";

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

	@BeforeClass
	public static void startCassandra() throws IOException {
		//generate temporary files
		tmpDir = CommonTestUtils.createTempDirectory();
		ClassLoader classLoader = CassandraAtLeastOnceSink.class.getClassLoader();
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

	@Override
	protected CassandraAtLeastOnceSink<Tuple3<String, Integer, Integer>> createSink() {
		return new CassandraAtLeastOnceSink<>(
			"127.0.0.1",
			INSERT_DATA_QUERY,
			new CassandraCommitter("127.0.0.1", "flink", "checkpoints"),
			TypeExtractor.getForObject(new Tuple3<>("", 0, 0)).createSerializer(new ExecutionConfig()));
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
		CassandraAtLeastOnceSink<Tuple3<String, Integer, Integer>> sink) {

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
		CassandraAtLeastOnceSink<Tuple3<String, Integer, Integer>> sink) {

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
		CassandraAtLeastOnceSink<Tuple3<String, Integer, Integer>> sink) {

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
}
