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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;

/**
 * CheckpointCommitter that saves information about completed checkpoints within a separate table in a cassandra
 * database.
 * <p/>
 * Entries are in the form |operator_id | subtask_id | last_completed_checkpoint|
 */
public class CassandraCommitter extends CheckpointCommitter {
	private ClusterBuilder builder;
	private transient Cluster cluster;
	private transient Session session;

	private String keySpace = "flink_auxiliary";
	private String table = "checkpoints_";

	private transient PreparedStatement updateStatement;
	private transient PreparedStatement selectStatement;

	private long lastCommittedCheckpointID = -1;

	public CassandraCommitter(ClusterBuilder builder) {
		this.builder = builder;
		ClosureCleaner.clean(builder, true);
	}

	public CassandraCommitter(ClusterBuilder builder, String keySpace) {
		this(builder);
		this.keySpace = keySpace;
	}

	/**
	 * Internally used to set the job ID after instantiation.
	 *
	 * @param id
	 * @throws Exception
	 */
	public void setJobId(String id) throws Exception {
		super.setJobId(id);
		table += id;
	}

	/**
	 * Generates the necessary tables to store information.
	 *
	 * @return
	 * @throws Exception
	 */
	@Override
	public void createResource() throws Exception {
		cluster = builder.getCluster();
		session = cluster.connect();

		session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s with replication={'class':'SimpleStrategy', 'replication_factor':1};", keySpace));
		session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (sink_id text, sub_id int, checkpoint_id bigint, PRIMARY KEY (sink_id, sub_id));", keySpace, table));

		try {
			session.close();
		} catch (Exception e) {
			LOG.error("Error while closing session.", e);
		}
		try {
			cluster.close();
		} catch (Exception e) {
			LOG.error("Error while closing cluster.", e);
		}
	}

	@Override
	public void open() throws Exception {
		if (builder == null) {
			throw new RuntimeException("No ClusterBuilder was set.");
		}
		cluster = builder.getCluster();
		session = cluster.connect();

		updateStatement = session.prepare(String.format("UPDATE %s.%s set checkpoint_id=? where sink_id='%s' and sub_id=%d;", keySpace, table, operatorId, subtaskId));
		selectStatement = session.prepare(String.format("SELECT checkpoint_id FROM %s.%s where sink_id='%s' and sub_id=%d;", keySpace, table, operatorId, subtaskId));

		session.execute(String.format("INSERT INTO %s.%s (sink_id, sub_id, checkpoint_id) values ('%s', %d, " + -1 + ") IF NOT EXISTS;", keySpace, table, operatorId, subtaskId));
	}

	@Override
	public void close() throws Exception {
		this.lastCommittedCheckpointID = -1;
		try {
			session.close();
		} catch (Exception e) {
			LOG.error("Error while closing session.", e);
		}
		try {
			cluster.close();
		} catch (Exception e) {
			LOG.error("Error while closing cluster.", e);
		}
	}

	@Override
	public void commitCheckpoint(long checkpointID) {
		session.execute(updateStatement.bind(checkpointID));
		this.lastCommittedCheckpointID = checkpointID;
	}

	@Override
	public boolean isCheckpointCommitted(long checkpointID) {
		if (this.lastCommittedCheckpointID == -1) {
			this.lastCommittedCheckpointID = session.execute(selectStatement.bind()).one().getLong("checkpoint_id");
		}
		return checkpointID <= this.lastCommittedCheckpointID;
	}
}
