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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Helper class to maintain a sharded database connection and get
 * {@link Connection}s and {@link PreparedStatement}s for keys.
 *
 */
public class ShardedConnection implements AutoCloseable, Serializable {

	private static final long serialVersionUID = 1L;
	private final Connection[] connections;
	private final int numShards;

	private final Partitioner partitioner;

	public ShardedConnection(List<String> shardUrls, String user, String password, Partitioner partitioner)
			throws SQLException {
		numShards = shardUrls.size();
		connections = new Connection[numShards];
		for (int i = 0; i < numShards; i++) {
			connections[i] = DriverManager.getConnection(shardUrls.get(i), user, password);
		}
		this.partitioner = partitioner;
	}

	public ShardedConnection(List<String> shardUrls, String user, String password) throws SQLException {
		this(shardUrls, user, password, new ModHashPartitioner());
	}

	public ShardedStatement prepareStatement(String sql) throws SQLException {
		return new ShardedStatement(sql);
	}

	public Connection[] connections() {
		return connections;
	}

	public Connection getForKey(Object key) {
		return connections[getShardIndex(key)];
	}

	public Connection getForIndex(int index) {
		if (index < numShards) {
			return connections[index];
		} else {
			throw new RuntimeException("Index out of range");
		}
	}

	public Connection getFirst() {
		return connections[0];
	}

	public int getNumShards() {
		return numShards;
	}

	@Override
	public void close() throws SQLException {
		if (connections != null) {
			for (Connection c : connections) {
				c.close();
			}
		}
	}

	public int getShardIndex(Object key) {
		return partitioner.getShardIndex(key, numShards);
	}

	public void setTransactionIsolation(int level) throws SQLException {
		for (Connection con : connections) {
			con.setTransactionIsolation(level);
		}
	}

	public class ShardedStatement implements AutoCloseable, Serializable {

		private static final long serialVersionUID = 1L;
		private final PreparedStatement[] statements = new PreparedStatement[numShards];

		public ShardedStatement(final String sql) throws SQLException {
			for (int i = 0; i < numShards; i++) {
				statements[i] = connections[i].prepareStatement(sql);
			}
		}

		public PreparedStatement getForKey(Object key) {
			return statements[getShardIndex(key)];
		}

		public PreparedStatement getForIndex(int index) {
			if (index < numShards) {
				return statements[index];
			} else {
				throw new RuntimeException("Index out of range");
			}
		}

		public PreparedStatement getFirst() {
			return statements[0];
		}

		@Override
		public void close() throws SQLException {
			if (statements != null) {
				for (PreparedStatement t : statements) {
					t.close();
				}
			}
		}

	}

	public interface Partitioner extends Serializable {
		int getShardIndex(Object key, int numShards);
	}

	public static class ModHashPartitioner implements Partitioner {

		private static final long serialVersionUID = 1L;

		@Override
		public int getShardIndex(Object key, int numShards) {
			return Math.abs(key.hashCode() % numShards);
		}

	}
}
