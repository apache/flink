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

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * CheckpointCommitter that saves information about completed checkpoints within a separate table in a cassandra
 * database.
 *
 * <p>Entries are in the form |operator_id | subtask_id | last_completed_checkpoint|
 */
public class CassandraCommitter extends CheckpointCommitter {

	private static final long serialVersionUID = 1L;

	private final ClusterBuilder builder;
	private transient Cluster cluster;
	private transient Session session;

	private String keySpace = "flink_auxiliary";
	private String table = "checkpoints_";

	/**
	 * A cache of the last committed checkpoint ids per subtask index. This is used to
	 * avoid redundant round-trips to Cassandra (see {@link #isCheckpointCommitted(int, long)}.
	 */
	private final Map<Integer, Long> lastCommittedCheckpoints = new HashMap<>();

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
	 */
	public void setJobId(String id) throws Exception {
		super.setJobId(id);
		table += id;
	}

	/**
	 * Generates the necessary tables to store information.
	 *
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
	}

	@Override
	public void close() throws Exception {
		this.lastCommittedCheckpoints.clear();
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
	public void commitCheckpoint(int subtaskIdx, long checkpointId) {
		String statement = String.format(
			"UPDATE %s.%s set checkpoint_id=%d where sink_id='%s' and sub_id=%d;",
			keySpace, table, checkpointId, operatorId, subtaskIdx);

		session.execute(statement);
		lastCommittedCheckpoints.put(subtaskIdx, checkpointId);
	}

	@Override
	public boolean isCheckpointCommitted(int subtaskIdx, long checkpointId) {
		// Pending checkpointed buffers are committed in ascending order of their
		// checkpoint id. This way we can tell if a checkpointed buffer was committed
		// just by asking the third-party storage system for the last checkpoint id
		// committed by the specified subtask.

		Long lastCommittedCheckpoint = lastCommittedCheckpoints.get(subtaskIdx);
		if (lastCommittedCheckpoint == null) {
			String statement = String.format(
				"SELECT checkpoint_id FROM %s.%s where sink_id='%s' and sub_id=%d;",
				keySpace, table, operatorId, subtaskIdx);

			Iterator<Row> resultIt = session.execute(statement).iterator();
			if (resultIt.hasNext()) {
				lastCommittedCheckpoint = resultIt.next().getLong("checkpoint_id");
				lastCommittedCheckpoints.put(subtaskIdx, lastCommittedCheckpoint);
			}
		}
		return lastCommittedCheckpoint != null && checkpointId <= lastCommittedCheckpoint;
	}
}
