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
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.WriteAheadSinkTestBase;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@SuppressWarnings("serial")
public class CassandraConnectorITCase extends WriteAheadSinkTestBase<Tuple3<String, Integer, Integer>, CassandraConnectorITCase.TestCassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>>> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraConnectorITCase.class);

	private static final String TABLE_NAME_PREFIX = "flink_";
	private static final String TABLE_NAME_VARIABLE = "$TABLE";
	private static final String CREATE_KEYSPACE_QUERY = "CREATE KEYSPACE flink WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};";
	private static final String CREATE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS flink." + TABLE_NAME_VARIABLE + " (id text PRIMARY KEY, counter int, batch_id int);";
	private static final String INSERT_DATA_QUERY = "INSERT INTO flink." + TABLE_NAME_VARIABLE + " (id, counter, batch_id) VALUES (?, ?, ?)";
	private static final String SELECT_DATA_QUERY = "SELECT * FROM flink." + TABLE_NAME_VARIABLE + ';';

	private static final boolean EMBEDDED = true;

	private static final ClusterBuilder builder = new ClusterBuilder() {
		@Override
		protected Cluster buildCluster(Cluster.Builder builder) {
			return builder
				.addContactPoint("127.0.0.1")
				.withSocketOptions(new SocketOptions().setConnectTimeoutMillis(30000))
				.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
				.withPoolingOptions(new PoolingOptions().setConnectionsPerHost(HostDistance.LOCAL, 32, 32).setMaxRequestsPerConnection(HostDistance.LOCAL, 2048).setPoolTimeoutMillis(15000))
				.withoutJMXReporting()
				.withoutMetrics().build();
		}
	};

	private static final List<Tuple3<String, Integer, Integer>> collection = new ArrayList<>();

	private static CassandraService cassandra;
	private static Cluster cluster;
	private static Session session;

	private static final Random random = new Random();
	private int tableID;

	@BeforeClass
	public static void generateCollection() {
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple3<>(UUID.randomUUID().toString(), i, 0));
		}
	}

	@BeforeClass
	public static void startCassandra() throws IOException {

		// check if we should run this test, current Cassandra version requires Java >= 1.8
		CommonTestUtils.assumeJava8();

		try {
			cassandra = new CassandraService();
		} catch (Exception e) {
			LOG.error("Failed to instantiate cassandra service.", e);
			Assert.fail("Failed to instantiate cassandra service.");
		}

		if (EMBEDDED) {
			cassandra.startProcess();
		}

		long start = System.currentTimeMillis();
		long deadline = start + 1000 * 30;
		while (true) {
			try {
				cluster = builder.getCluster();
				session = cluster.connect();
				break;
			} catch (Exception e) {
				if (System.currentTimeMillis() > deadline) {
					throw e;
				}
				try {
					Thread.sleep(1000);
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
		session.execute(CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + tableID));
	}

	@AfterClass
	public static void closeCassandra() {
		if (session != null) {
			session.close();
		}

		if (cluster != null) {
			cluster.close();
		}

		if (EMBEDDED) {
			if (cassandra != null) {
				cassandra.destroy();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Exactly-once Tests
	// ------------------------------------------------------------------------

	@Override
	protected TestCassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> createSink() throws Exception {
		return new TestCassandraTupleWriteAheadSink<>(
			TABLE_NAME_PREFIX + tableID,
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
	protected void verifyResultsIdealCircumstances(
		OneInputStreamOperatorTestHarness<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> harness,
		TestCassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

		ResultSet result = session.execute(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, sink.tableName));
		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 60; x++) {
			list.add(x);
		}

		for (Row s : result) {
			list.remove(Integer.valueOf(s.getInt("counter")));
		}
		Assert.assertTrue("The following ID's were not found in the ResultSet: " + list, list.isEmpty());
	}

	@Override
	protected void verifyResultsDataPersistenceUponMissedNotify(
		OneInputStreamOperatorTestHarness<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> harness,
		TestCassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

		ResultSet result = session.execute(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, sink.tableName));
		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 60; x++) {
			list.add(x);
		}

		for (Row s : result) {
			list.remove(Integer.valueOf(s.getInt("counter")));
		}
		Assert.assertTrue("The following ID's were not found in the ResultSet: " + list, list.isEmpty());
	}

	@Override
	protected void verifyResultsDataDiscardingUponRestore(
		OneInputStreamOperatorTestHarness<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> harness,
		TestCassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {
		ResultSet result = session.execute(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, sink.tableName));
		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 20; x++) {
			list.add(x);
		}
		for (int x = 41; x <= 60; x++) {
			list.add(x);
		}

		for (Row s : result) {
			list.remove(Integer.valueOf(s.getInt("counter")));
		}
		Assert.assertTrue("The following ID's were not found in the ResultSet: " + list, list.isEmpty());
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

		CassandraCommitter cc4 = new CassandraCommitter(builder, "flink_auxiliary_cc");
		cc4.setJobId(jobID);
		cc4.setOperatorId("operator");

		cc4.open();

		//verify that checkpoint data is not destroyed within open/close and not reliant on internally cached data
		Assert.assertTrue(cc4.isCheckpointCommitted(0, 1));
		Assert.assertFalse(cc4.isCheckpointCommitted(0, 2));

		cc4.close();
	}

	// ------------------------------------------------------------------------
	//  At-least-once Tests
	// ------------------------------------------------------------------------

	@Test
	public void testCassandraTupleAtLeastOnceSink() throws Exception {
		CassandraTupleSink<Tuple3<String, Integer, Integer>> sink = new CassandraTupleSink<>(INSERT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + tableID), builder);

		sink.open(new Configuration());

		for (Tuple3<String, Integer, Integer> value : collection) {
			sink.send(value);
		}

		sink.close();

		synchronized (sink.updatesPending) {
			if (sink.updatesPending.get() != 0) {
				sink.updatesPending.wait();
			}
		}

		ResultSet rs = session.execute(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + tableID));
		try {
			Assert.assertEquals(20, rs.all().size());
		} catch (Throwable e) {
			LOG.error("test failed.", e);
		}
	}

	@Test
	public void testCassandraPojoAtLeastOnceSink() throws Exception {
		session.execute(CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, "test"));

		CassandraPojoSink<Pojo> sink = new CassandraPojoSink<>(Pojo.class, builder);

		sink.open(new Configuration());

		for (int x = 0; x < 20; x++) {
			sink.send(new Pojo(UUID.randomUUID().toString(), x, 0));
		}

		sink.close();

		synchronized (sink.updatesPending) {
			while (sink.updatesPending.get() != 0) {
				sink.updatesPending.wait();
			}
		}

		ResultSet rs = session.execute(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, "test"));
		try {
			Assert.assertEquals(20, rs.all().size());
		} catch (Throwable e) {
			LOG.error("test failed.", e);
		}
	}

	@Test
	public void testCassandraBatchFormats() throws Exception {
		OutputFormat<Tuple3<String, Integer, Integer>> sink = new CassandraOutputFormat<>(INSERT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + tableID), builder);
		sink.configure(new Configuration());
		sink.open(0, 1);

		for (Tuple3<String, Integer, Integer> value : collection) {
			sink.writeRecord(value);
		}

		sink.close();

		InputFormat<Tuple3<String, Integer, Integer>, InputSplit> source = new CassandraInputFormat<>(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + tableID), builder);
		source.configure(new Configuration());
		source.open(null);

		List<Tuple3<String, Integer, Integer>> result = new ArrayList<>();

		while (!source.reachedEnd()) {
			result.add(source.nextRecord(new Tuple3<String, Integer, Integer>()));
		}

		source.close();
		try {
			Assert.assertEquals(20, result.size());
		} catch (Throwable e) {
			LOG.error("test failed.", e);
		}
	}

	protected static class TestCassandraTupleWriteAheadSink<IN extends Tuple> extends CassandraTupleWriteAheadSink<IN> {
		private final String tableName;

		private TestCassandraTupleWriteAheadSink(String tableName, TypeSerializer<IN> serializer, ClusterBuilder builder, CheckpointCommitter committer) throws Exception {
			super(INSERT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, tableName), serializer, builder, committer);
			this.tableName = tableName;
		}
	}
}
