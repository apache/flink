/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connectors.cassandra;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

import com.datastax.driver.core.Session;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.cassandra.batch.CassandraInputFormat;
import org.apache.flink.connectors.cassandra.batch.CassandraOutputFormat;
import org.apache.flink.connectors.cassandra.streaming.CassandraMapperSink;
import org.apache.flink.connectors.cassandra.streaming.CassandraSink;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest({Pojo.class})
@PowerMockIgnore("javax.management.*")
public class CassandraConnectorTest extends StreamingMultipleProgramsTestBase {

	private static File tmpDir;

	private static EmbeddedCassandraService cassandra;
	private static Cluster.Builder builder;
	private static Session session;

	private static final long COUNT = 20;

	//
	//  QUERY
	//
	private static final String CREATE_KEYSPACE = "CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
	private static final String DROP_KEYSPACE = "DROP KEYSPACE test;";

	private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS test.tuplesink(id bigint PRIMARY KEY, value text);";
	private static final String INSERT_QUERY = "INSERT INTO test.tuplesink (id,value) VALUES (?,?);";
	private static final String SELECT_QUERY = "SELECT * FROM test.tuplesink;";
	private static final String DROP_TABLE = "DROP TABLE test.tuplesink;";

	private static final String CREATE_TABLE_MAPPER = "CREATE TABLE IF NOT EXISTS test.mappersink(id bigint,value text,PRIMARY KEY(id))";
	private static final String SELECT_QUERY_MAPPER = "SELECT * FROM test.mappersink;";
	private static final String DROP_TABLE_MAPPER = "DROP TABLE test.mappersink;";

	private static final ArrayList<Tuple2<Long,String>> collection = new ArrayList<>(20);
	static {
		for (long i = 0; i < 20; i++) {
			collection.add(new Tuple2<>(i, "cassandra-" + i));
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

	@BeforeClass
	public static void startCassandra() throws IOException {

		//generate temporary files
		tmpDir = CommonTestUtils.createTempDirectory();
		ClassLoader classLoader = CassandraConnectorTest.class.getClassLoader();
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


		cassandra = new EmbeddedCassandraService();
		cassandra.start();


		builder = Cluster.builder().addContactPoint("127.0.0.1");
		session = builder.build().connect();

		session.execute(CREATE_KEYSPACE);
	}

	//
	//	CassandraSink.java
	//

	@Test(expected = IllegalArgumentException.class)
	public void queryNotSet() {
		new CassandraSink<Tuple2<Long, String>>(null) {

			@Override
			public Builder configureCluster(Builder cluster) {
                return CassandraConnectorTest.builder;
			}
		};
	}

	@Test
	public void cassandraSink() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<Long, String>> source = env.fromCollection(collection);
		
		source.addSink(new CassandraSink<Tuple2<Long, String>>(CREATE_TABLE, INSERT_QUERY) {

			@Override
			public Builder configureCluster(Builder cluster) {
                return CassandraConnectorTest.builder;
            }
		});

		env.execute();

		ResultSet rs =  session.execute(SELECT_QUERY);
		Assert.assertEquals(COUNT, rs.all().size());
		session.execute(DROP_TABLE);
	}


	@Test(expected = JobExecutionException.class)
	public void runtimeExceptionCreateQuery() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		DataStreamSource<Tuple2<Long, String>> source = env.fromCollection(collection);
		source.addSink(new CassandraSink<Tuple2<Long, String>>(CREATE_TABLE, INSERT_QUERY) {

			@Override
			public Builder configureCluster(Builder cluster) {
				return CassandraConnectorTest.builder;
			}
		});

		env.execute();

	}

	//
	// CassandraMapperSink.java
	//

	@Test(expected = NullPointerException.class)
	public void clazzNotSet() {

		class Foo implements Serializable {
		}
		new CassandraMapperSink<Foo>(null) {

			@Override
			public Builder configureCluster(Builder cluster) {
                return CassandraConnectorTest.builder;
			}
		};
	}

	@Test
	public void cassandraMapperSink() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.setParallelism(1);

		DataStreamSource<Pojo> source = env
				.addSource(new SourceFunction<Pojo>() {

					private boolean running = true;
					private volatile long cnt = 0;

					@Override
					public void run(SourceContext<Pojo> ctx) throws Exception {
						while (running) {
							ctx.collect(new Pojo(cnt, "cassandra-" + cnt));
							cnt++;
							if (cnt == COUNT) {
								cancel();
							}
						}
					}

					@Override
					public void cancel() {
                        running = false;
                    }
				});

		source.addSink(new CassandraMapperSink<Pojo>(CREATE_TABLE_MAPPER,Pojo.class) {

			@Override
			public Builder configureCluster(Builder cluster) {
                return CassandraConnectorTest.builder;
            }
		});

		env.execute();

		ResultSet rs = session.execute(SELECT_QUERY_MAPPER);
		Assert.assertEquals(COUNT,rs.all().size());
		session.execute(DROP_TABLE_MAPPER);
	}


	//
	//	CassandraOutputFormat.java
	//	CassandraInputFormat.java
	//

	@Test
	public void batch() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long,String>> dataSet = env.fromCollection(collection);

		dataSet.output(new CassandraOutputFormat<Tuple2<Long,String>>(CREATE_TABLE,INSERT_QUERY) {

			@Override
			public Builder configureCluster(Builder cluster) {
				return builder;
			}
		});

		DataSet<Tuple2<Long,String>> inputDS = env.createInput(new CassandraInputFormat<Tuple2<Long,String>>(SELECT_QUERY) {

			@Override
			public Builder configureCluster(Builder cluster) {
				return builder;
			}
		});


		long count =inputDS.count();
		Assert.assertEquals(count, 20L);
		session.execute(DROP_TABLE);
	}

	@AfterClass
	public static void closeCassandra() {
		session.executeAsync(DROP_KEYSPACE);
		session.close();
		cassandra.stop();

		tmpDir.delete();
	}
}