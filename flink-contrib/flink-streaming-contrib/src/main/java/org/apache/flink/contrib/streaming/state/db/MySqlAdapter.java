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

package org.apache.flink.contrib.streaming.state.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Adapter for bridging inconsistencies between the different SQL
 * implementations. The default implementation has been tested to work well with
 * MySQL
 *
 */
public class MySqlAdapter implements DbAdapter {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(MySqlAdapter.class);

	private static final byte[] CLEANUP_MARKER = new byte[0];
	private final long SLEEP_BETWEEN_CLEANUP_CHECKS = 3000;
	private long maxWaitForCleanup = 120000;
	private boolean cleanupDisabled = false;

	public MySqlAdapter setCleanupTimeout(long millis) {
		this.maxWaitForCleanup = millis;
		return this;
	}

	public MySqlAdapter disableCleanup() {
		this.cleanupDisabled = true;
		return this;
	}

	// -----------------------------------------------------------------------------
	// Non-partitioned state checkpointing
	// -----------------------------------------------------------------------------

	@Override
	public void createCheckpointsTable(String appId, Connection con) throws SQLException {
		if (!isTableCreated(con, "checkpoints_" + appId)) {
			try (Statement smt = con.createStatement()) {
				smt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS checkpoints_" + appId
								+ " ("
								+ "checkpointId bigint, "
								+ "timestamp bigint, "
								+ "handleId bigint,"
								+ "checkpoint longblob,"
								+ "PRIMARY KEY (handleId)"
								+ ")");
			}
			LOG.debug("Checkpoints table created for {}", appId);
		} else {
			LOG.debug("Checkpoints table alredy created for {}", appId);
		}
	}

	@Override
	public PreparedStatement prepareCheckpointInsert(String appId, Connection con) throws SQLException {
		return con.prepareStatement(
				"INSERT INTO checkpoints_" + appId
						+ " (checkpointId, timestamp, handleId, checkpoint) VALUES (?,?,?,?)");
	}

	@Override
	public void setCheckpointInsertParams(String appId, PreparedStatement insertStatement, long checkpointId,
			long timestamp, long handleId, byte[] checkpoint) throws SQLException {
		insertStatement.setLong(1, checkpointId);
		insertStatement.setLong(2, timestamp);
		insertStatement.setLong(3, handleId);
		insertStatement.setBytes(4, checkpoint);
	}

	@Override
	public byte[] getCheckpoint(String appId, Connection con, long checkpointId, long checkpointTs, long handleId)
			throws SQLException {
		try (Statement smt = con.createStatement()) {
			ResultSet rs = smt.executeQuery(
					"SELECT checkpoint FROM checkpoints_" + appId
							+ " WHERE handleId = " + handleId);
			if (rs.next()) {
				return rs.getBytes(1);
			} else {
				throw new SQLException("Checkpoint cannot be found in the database.");
			}
		}
	}

	@Override
	public void deleteCheckpoint(String appId, Connection con, long checkpointId, long checkpointTs, long handleId)
			throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"DELETE FROM checkpoints_" + appId
							+ " WHERE handleId = " + handleId);
		}
	}

	@Override
	public void disposeAllStateForJob(String appId, Connection con) throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"DROP TABLE checkpoints_" + appId);
		}
	}

	// -----------------------------------------------------------------------------
	// Partitioned state checkpointing
	// -----------------------------------------------------------------------------

	@Override
	public void createKVStateTable(String stateId, Connection con) throws SQLException {
		con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		validateStateId(stateId);
		if (!isTableCreated(con, stateId)) {
			try (Statement smt = con.createStatement()) {
				smt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS " + stateId
								+ " ("
								+ "timestamp bigint, "
								+ "k varbinary(256), "
								+ "v longblob, "
								+ "PRIMARY KEY (k, timestamp) "
								+ ")");
			}
			LOG.debug("KV checkpoint table created for {}", stateId);
		} else {
			LOG.debug("KV checkpoint table already created for {}", stateId);
		}
	}

	@Override
	public String prepareKVCheckpointInsert(String stateId) throws SQLException {
		validateStateId(stateId);
		return "INSERT IGNORE INTO " + stateId + " (timestamp, k, v) VALUES (?,?,?)";
	}

	@Override
	public String prepareKeyLookup(String stateId) throws SQLException {
		validateStateId(stateId);
		return "SELECT v"
				+ " FROM " + stateId
				+ " WHERE k = ?"
				+ " AND timestamp <= ?"
				+ " ORDER BY timestamp DESC LIMIT 1";
	}

	@Override
	public byte[] lookupKey(String stateId, PreparedStatement lookupStatement, byte[] key, long lookupTs)
			throws SQLException {
		lookupStatement.setBytes(1, key);
		lookupStatement.setLong(2, lookupTs);

		ResultSet res = lookupStatement.executeQuery();

		if (res.next()) {
			return res.getBytes(1);
		} else {
			return null;
		}
	}

	@Override
	public void cleanupFailedCheckpoints(final DbBackendConfig conf, final String stateId, final Connection con,
			final long checkpointTs, final long recoveryTs) throws SQLException {

		if (cleanupDisabled) {
			LOG.info(
					"Failed state cleanup has been disabled, this violates exactly-once semantics and might lead to duplicate state updates.");
		} else {
			try {
				if (startCleanup(conf, stateId, con, recoveryTs)) {
					// Only one task should succeed here
					LOG.debug("Removing failed state updates for " + stateId);

					// Execute the cleanup and mark finished (this should be
					// retried if it fails for some reason)
					SQLRetrier.retry(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							try (Statement smt = con.createStatement()) {
								smt.executeUpdate("DELETE FROM " + stateId
										+ " WHERE timestamp > " + checkpointTs
										+ " AND timestamp < " + recoveryTs);
							}
							finishCleanup(conf, stateId, con, recoveryTs);
							return null;
						}
					}, conf.getMaxNumberOfSqlRetries());

					LOG.debug("Cleanup performed successfully for " + stateId);
				} else {
					LOG.debug("Cleanup is performed at another task for " + stateId);
					// We need to wait until the cleanup task finishes
					waitForCleanup(conf, stateId, con, recoveryTs);
				}
			} catch (IOException e) {
				// We don't want to propagate any exceptions here as that would
				// just
				// cause the state to retrty cleanup
				throw new RuntimeException("Could not clean up failed state", e);
			}
		}
	}

	/**
	 * Tries to start the cleanup by inserting a marker for the current recovery
	 * timestamp. Returns true if this is the first task to start and returns
	 * false if some other task has already inserted the marker before.
	 */
	private boolean startCleanup(DbBackendConfig conf, final String stateId, final Connection con,
			final long recoveryTs)
					throws IOException {
		return SQLRetrier.retry(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				try (PreparedStatement smt = con
						.prepareStatement("INSERT IGNORE INTO " + stateId + " (timestamp, k, v) VALUES (?,?,?)")) {
					setKvInsertParams(stateId, smt, recoveryTs, CLEANUP_MARKER, CLEANUP_MARKER);

					// This statement returns 1 if it can successfully insert
					// the marker otherwise it returns 0.
					return smt.executeUpdate() == 1;
				}
			}
		}, conf.getMaxNumberOfSqlRetries());
	}

	/**
	 * Update the value of the cleanup marker to NULL indicating that it is
	 * finished and everyone can start processing.
	 */
	private void finishCleanup(final DbBackendConfig conf, final String stateId, final Connection con,
			final long recoveryTs) throws IOException {

		SQLRetrier.retry(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				try (PreparedStatement smt = con
						.prepareStatement("UPDATE " + stateId + " SET v=? WHERE k=? AND timestamp=" + recoveryTs)) {
					smt.setNull(1, Types.BLOB);
					smt.setBytes(2, CLEANUP_MARKER);
					smt.executeUpdate();
				}
				return null;
			}
		}, conf.getMaxNumberOfSqlRetries());
	}

	/**
	 * Checks whether the cleanup marker's value has been updated to NULL
	 * indicating that the cleanup is finished.
	 */
	private boolean isCleanupFinished(final DbBackendConfig conf, final String stateId, final Connection con,
			final long recoveryTs)
					throws IOException {

		return SQLRetrier.retry(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				try (PreparedStatement smt = con.prepareStatement("SELECT v FROM " + stateId
						+ " WHERE k = ? AND timestamp = " + recoveryTs)) {

					smt.setBytes(1, CLEANUP_MARKER);
					ResultSet res = smt.executeQuery();

					if (res.next()) {
						return res.getBytes(1) == null;
					} else {
						throw new RuntimeException("Error in cleanup logic");
					}
				}
			}
		}, conf.getMaxNumberOfSqlRetries());
	}

	/**
	 * Wait until the cleanup task has finished with deleting the failed records
	 * from the database. This method periodically checks whether the cleanup
	 * has finished and sleeps between checks.
	 */
	private void waitForCleanup(DbBackendConfig conf, final String stateId, final Connection con, final long recoveryTs)
			throws IOException {
		boolean wait = true;
		long slept = 0;
		while (wait) {
			try {
				if (isCleanupFinished(conf, stateId, con, recoveryTs)) {
					wait = false;
				} else {
					LOG.debug("Some other task is already cleaning, sleeping...");
					Thread.sleep(SLEEP_BETWEEN_CLEANUP_CHECKS);
					slept += SLEEP_BETWEEN_CLEANUP_CHECKS;
					if (slept >= maxWaitForCleanup) {
						throw new IOException(
								"Clean up did not finish in " + maxWaitForCleanup / 1000 + " seconds.");
					}
				}
			} catch (InterruptedException e) {
				wait = false;
			}
		}
	}

	@Override
	public void compactKvStates(String stateId, Connection con, long lowerId, long upperId)
			throws SQLException {
		validateStateId(stateId);

		try (Statement smt = con.createStatement()) {
			smt.executeUpdate("DELETE state.* FROM " + stateId + " AS state"
					+ " JOIN"
					+ " ("
					+ " 	SELECT MAX(timestamp) AS maxts, k FROM " + stateId
					+ " 	WHERE timestamp BETWEEN " + lowerId + " AND " + upperId
					+ " 	GROUP BY k"
					+ " ) m"
					+ " ON state.k = m.k"
					+ " AND state.timestamp >= " + lowerId);
		}
	}

	@Override
	public void insertBatch(final String stateId, final DbBackendConfig conf,
			final Connection con, final PreparedStatement insertStatement, final long checkpointTs,
			final List<Tuple2<byte[], byte[]>> toInsert) throws IOException {

		// As we are inserting with INSERT IGNORE statements, we can simply
		// retry on failed batches without any rollback logic
		SQLRetrier.retry(new Callable<Void>() {
			public Void call() throws Exception {
				for (Tuple2<byte[], byte[]> kv : toInsert) {
					setKvInsertParams(stateId, insertStatement, checkpointTs, kv.f0, kv.f1);
					insertStatement.addBatch();
				}
				insertStatement.executeBatch();
				insertStatement.clearBatch();
				if (LOG.isDebugEnabled()) {
					LOG.debug("Succesfully inserted {} values for {}", toInsert.size(), stateId);
				}
				return null;
			}
		}, conf.getMaxNumberOfSqlRetries(), conf.getSleepBetweenSqlRetries());
	}

	/**
	 * Sets the proper parameters for the {@link PreparedStatement} used for
	 * inserts.
	 */
	protected void setKvInsertParams(String stateId, PreparedStatement insertStatement, long checkpointTs,
			byte[] key, byte[] value) throws SQLException {
		insertStatement.setLong(1, checkpointTs);
		insertStatement.setBytes(2, key);
		if (value != null) {
			insertStatement.setBytes(3, value);
		} else {
			insertStatement.setNull(3, Types.BLOB);
		}
	}

	/**
	 * Checks whether the given table is already created in the database.
	 * 
	 */
	private boolean isTableCreated(Connection con, String tableName) throws SQLException {
		return con.getMetaData().getTables(null, null, tableName, null).next();
	}

	/**
	 * Tries to avoid SQL injection with weird state names.
	 */
	protected static void validateStateId(String name) {
		if (!name.matches("[a-zA-Z0-9_]+")) {
			throw new RuntimeException("State name contains invalid characters.");
		}
	}

}
