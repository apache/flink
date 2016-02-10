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
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;

/**
 * CheckpointCommitter that saves information about completed checkpoints within a separate table in a cassandra
 * database.
 *
 * Entries are in the form |operator_id | subtask_id | last_completed_checkpoint|
 */
public class CassandraCommitter extends CheckpointCommitter {
	private final String host;
	private final String keyspace;
	private final String table;

	private transient Cluster cluster;
	private transient Session session;

	public CassandraCommitter(String host, String keyspace, String table) {
		this.host = host;
		this.keyspace = keyspace;
		this.table = table;
	}

	@Override
	public void open() throws Exception {
		cluster = Cluster.builder().addContactPoint(host).build();
		session = cluster.connect();

		session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " with replication={'class':'SimpleStrategy', 'replication_factor':3};");
		session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " (sink_id text, sub_id int, checkpoint_id bigint, PRIMARY KEY (sink_id, sub_id));");
		session.executeAsync("INSERT INTO " + keyspace + "." + table + " (sink_id, sub_id, checkpoint_id) values ('" + operatorId + "', " + subtaskId + ", " + -1 + ");");
	}

	@Override
	public void close() throws Exception {
		session.executeAsync("DELETE FROM " + keyspace + "." + table + " where sink_id='" + operatorId + "' and sub_id=" + subtaskId + ";");
		session.close();
		cluster.close();
	}

	@Override
	public void commitCheckpoint(long checkpointID) {
		SimpleStatement s = new SimpleStatement("UPDATE " + keyspace + "." + table + " set checkpoint_id=" + checkpointID + " where sink_id='" + operatorId + "' and sub_id=" + subtaskId + ";");
		s.setConsistencyLevel(ConsistencyLevel.ALL);
		session.executeAsync(s);
	}

	@Override
	public boolean isCheckpointCommitted(long checkpointID) {
		SimpleStatement s = new SimpleStatement("SELECT checkpoint_id FROM " + keyspace + "." + table + " where sink_id='" + operatorId + "' and sub_id=" + subtaskId + ";");
		s.setConsistencyLevel(ConsistencyLevel.ALL);
		long lastId = session.execute(s).one().getLong("checkpoint_id");
		return checkpointID <= lastId;
	}
}
