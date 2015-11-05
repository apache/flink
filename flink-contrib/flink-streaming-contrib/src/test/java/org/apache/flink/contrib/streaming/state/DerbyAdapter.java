/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Adapter for the Derby JDBC driver which has slightly restricted CREATE TABLE
 * and SELECT semantics compared to the default assumptions.
 * 
 */
public class DerbyAdapter extends MySqlAdapter {

	private static final long serialVersionUID = 1L;

	/**
	 * We need to override this method as Derby does not support the
	 * "IF NOT EXISTS" clause at table creation
	 */
	@Override
	public void createCheckpointsTable(String jobId, Connection con) throws SQLException {

		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"CREATE TABLE checkpoints_" + jobId
							+ " ("
							+ "checkpointId bigint, "
							+ "timestamp bigint, "
							+ "handleId bigint,"
							+ "checkpoint blob,"
							+ "PRIMARY KEY (handleId)"
							+ ")");
		} catch (SQLException se) {
			if (se.getSQLState().equals("X0Y32")) {
				// table already created, ignore
			} else {
				throw se;
			}
		}
	}

	/**
	 * We need to override this method as Derby does not support the
	 * "IF NOT EXISTS" clause at table creation
	 */
	@Override
	public void createKVStateTable(String stateId, Connection con) throws SQLException {

		validateStateId(stateId);
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"CREATE TABLE kvstate_" + stateId
							+ " ("
							+ "id bigint, "
							+ "k varchar(256) for bit data, "
							+ "v blob, "
							+ "PRIMARY KEY (k, id)"
							+ ")");
		} catch (SQLException se) {
			if (se.getSQLState().equals("X0Y32")) {
				// table already created, ignore
			} else {
				throw se;
			}
		}
	}

	/**
	 * We need to override this method as Derby does not support "LIMIT n" for
	 * select statements.
	 */
	@Override
	public String prepareKeyLookup(String stateId) throws SQLException {
		validateStateId(stateId);
		return "SELECT v " + "FROM kvstate_" + stateId
				+ " WHERE k = ? "
				+ " AND id <= ? "
				+ "ORDER BY id DESC";
	}

	@Override
	public void compactKvStates(String stateId, Connection con, long lowerBound, long upperBound)
			throws SQLException {
		validateStateId(stateId);

		try (Statement smt = con.createStatement()) {
			smt.executeUpdate("DELETE FROM kvstate_" + stateId + " t1"
					+ " WHERE EXISTS"
					+ " ("
					+ " 	SELECT * FROM kvstate_" + stateId + " t2"
					+ " 	WHERE t2.k = t1.k"
					+ "		AND t2.id > t1.id"
					+ " 	AND t2.id <=" + upperBound
					+ "		AND t2.id >= " + lowerBound
					+ " )");
		}
	}

	@Override
	public String prepareKVCheckpointInsert(String stateId) throws SQLException {
		validateStateId(stateId);
		return "INSERT INTO kvstate_" + stateId + " (id, k, v) VALUES (?,?,?)";
	}

	@Override
	public void insertBatch(final String stateId, final DbBackendConfig conf,
			final Connection con, final PreparedStatement insertStatement, final long checkpointId,
			final List<Tuple2<byte[], byte[]>> toInsert) throws IOException {

		try (PreparedStatement smt = con
				.prepareStatement("UPDATE kvstate_" + stateId + " SET v=? WHERE k=? AND id=?")) {
			for (final Tuple2<byte[], byte[]> kv : toInsert) {
				SQLRetrier.retry(new Callable<Void>() {
					public Void call() throws Exception {
						try {
							setKVInsertParams(stateId, insertStatement, checkpointId, kv.f0, kv.f1);
							insertStatement.executeUpdate();
						} catch (SQLException e) {
							if (kv.f0 != null) {
								smt.setBytes(1, kv.f1);
							} else {
								smt.setNull(1, Types.BLOB);
							}
							smt.setBytes(2, kv.f0);
							smt.setLong(3, checkpointId);
							smt.executeUpdate();
						}
						return null;
					}
				}, conf.getMaxNumberOfSqlRetries(), conf.getSleepBetweenSqlRetries());

			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private void setKVInsertParams(String stateId, PreparedStatement insertStatement, long checkpointId,
			byte[] key, byte[] value) throws SQLException {
		insertStatement.setLong(1, checkpointId);
		insertStatement.setBytes(2, key);
		if (value != null) {
			insertStatement.setBytes(3, value);
		} else {
			insertStatement.setNull(3, Types.BLOB);
		}
	}
}
