/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;

public interface DbAdapter extends Serializable {

	/**
	 * Initialize tables for storing non-partitioned checkpoints for the given
	 * job id and database connection.
	 * 
	 */
	void createCheckpointsTable(String jobId, Connection con) throws SQLException;

	/**
	 * Checkpoints will be inserted in the database using prepared statements.
	 * This methods should prepare and return the statement that will be used
	 * later to insert using the given connection.
	 * 
	 */
	PreparedStatement prepareCheckpointInsert(String jobId, Connection con) throws SQLException;

	/**
	 * Set the {@link PreparedStatement} parameters for the statement returned
	 * by {@link #prepareCheckpointInsert(String, Connection)}.
	 * 
	 * @param jobId
	 *            Id of the current job.
	 * @param insertStatement
	 *            Statement returned by
	 *            {@link #prepareCheckpointInsert(String, Connection)}.
	 * @param checkpointId
	 *            Global checkpoint id.
	 * @param timestamp
	 *            Global checkpoint timestamp.
	 * @param handleId
	 *            Unique id assigned to this state checkpoint (should be primary
	 *            key).
	 * @param checkpoint
	 *            The serialized checkpoint.
	 * @throws SQLException
	 */
	void setCheckpointInsertParams(String jobId, PreparedStatement insertStatement, long checkpointId,
			long timestamp, long handleId, byte[] checkpoint) throws SQLException;

	/**
	 * Retrieve the serialized checkpoint data from the database.
	 * 
	 * @param jobId
	 *            Id of the current job.
	 * @param con
	 *            Database connection
	 * @param checkpointId
	 *            Global checkpoint id.
	 * @param checkpointTs
	 *            Global checkpoint timestamp.
	 * @param handleId
	 *            Unique id assigned to this state checkpoint (should be primary
	 *            key).
	 * @return The byte[] corresponding to the checkpoint or null if missing.
	 * @throws SQLException
	 */
	byte[] getCheckpoint(String jobId, Connection con, long checkpointId, long checkpointTs, long handleId)
			throws SQLException;

	/**
	 * Remove the given checkpoint from the database.
	 * 
	 * @param jobId
	 *            Id of the current job.
	 * @param con
	 *            Database connection
	 * @param checkpointId
	 *            Global checkpoint id.
	 * @param checkpointTs
	 *            Global checkpoint timestamp.
	 * @param handleId
	 *            Unique id assigned to this state checkpoint (should be primary
	 *            key).
	 * @return The byte[] corresponding to the checkpoint or null if missing.
	 * @throws SQLException
	 */
	void deleteCheckpoint(String jobId, Connection con, long checkpointId, long checkpointTs, long handleId)
			throws SQLException;

	/**
	 * Remove all states for the given JobId, by for instance dropping the
	 * entire table.
	 * 
	 * @throws SQLException
	 */
	void disposeAllStateForJob(String jobId, Connection con) throws SQLException;

	/**
	 * Initialize the necessary tables for the given stateId. The state id
	 * consist of the JobId+OperatorId+StateName.
	 * 
	 */
	void createKVStateTable(String stateId, Connection con) throws SQLException;

	/**
	 * Prepare the the statement that will be used to insert key-value pairs in
	 * the database.
	 * 
	 */
	String prepareKVCheckpointInsert(String stateId) throws SQLException;

	/**
	 * Prepare the statement that will be used to lookup keys from the database.
	 * Keys and values are assumed to be byte arrays.
	 * 
	 */
	String prepareKeyLookup(String stateId) throws SQLException;

	/**
	 * Retrieve the latest value from the database for a given key and
	 * checkpointId.
	 * 
	 * @param stateId
	 *            Unique identifier of the kvstate (usually the table name).
	 * @param lookupStatement
	 *            The statement returned by
	 *            {@link #prepareKeyLookup(String, Connection)}.
	 * @param key
	 *            The key to lookup.
	 * @return The latest valid value for the key.
	 * @throws SQLException
	 */
	byte[] lookupKey(String stateId, PreparedStatement lookupStatement, byte[] key, long lookupId)
			throws SQLException;

	/**
	 * Clean up states between the current and next checkpoint id. Everything
	 * with larger than current and smaller than next should be removed.
	 * 
	 */
	void cleanupFailedCheckpoints(String stateId, Connection con, long checkpointId,
			long nextId) throws SQLException;

	/**
	 * Insert a list of Key-Value pairs into the database. The suggested
	 * approach is to use idempotent inserts(updates) as 1 batch operation.
	 * 
	 */
	void insertBatch(String stateId, DbBackendConfig conf, Connection con, PreparedStatement insertStatement,
			long checkpointId, List<Tuple2<byte[], byte[]>> toInsert) throws IOException;

	/**
	 * Compact the states between two checkpoint ids by only keeping the most
	 * recent.
	 */
	void compactKvStates(String kvStateId, Connection con, long lowerId, long upperId) throws SQLException;

}