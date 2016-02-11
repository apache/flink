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

package org.apache.flink.contrib.streaming.state.db;

import java.sql.SQLException;
import java.util.List;

import org.apache.flink.contrib.streaming.state.KvStateConfig;
import org.apache.flink.contrib.streaming.state.db.ShardedConnection.Partitioner;

import com.google.common.collect.Lists;

/**
 * 
 * Configuration object for {@link DbStateBackend}, containing information to
 * shard and connect to the databases that will store the state checkpoints.
 *
 */
public class DbBackendConfig extends KvStateConfig<DbBackendConfig> {

	private static final long serialVersionUID = 1L;

	// Database connection properties
	private final String userName;
	private final String userPassword;
	private final List<String> shardUrls;

	// JDBC Driver + DbAdapter information
	private DbAdapter dbAdapter = new MySqlAdapter();
	private String JDBCDriver = null;

	private int maxNumberOfSqlRetries = 5;
	private int sleepBetweenSqlRetries = 100;

	protected int maxKvInsertBatchSize = 1000;

	private Partitioner shardPartitioner;

	/**
	 * Creates a new sharded database state backend configuration with the given
	 * parameters and default {@link MySqlAdapter}.
	 * 
	 * @param dbUserName
	 *            The username used to connect to the database at the given url.
	 * @param dbUserPassword
	 *            The password used to connect to the database at the given url
	 *            and username.
	 * @param dbShardUrls
	 *            The list of JDBC urls of the databases that will be used as
	 *            shards for the state backend. Sharding of the state will
	 *            happen based on the subtask index of the given task.
	 */
	public DbBackendConfig(String dbUserName, String dbUserPassword, List<String> dbShardUrls) {
		this.userName = dbUserName;
		this.userPassword = dbUserPassword;
		this.shardUrls = dbShardUrls;
	}

	/**
	 * Creates a new database state backend configuration with the given
	 * parameters and default {@link MySqlAdapter}.
	 * 
	 * @param dbUserName
	 *            The username used to connect to the database at the given url.
	 * @param dbUserPassword
	 *            The password used to connect to the database at the given url
	 *            and username.
	 * @param dbUrl
	 *            The JDBC url of the database for example
	 *            "jdbc:mysql://localhost:3306/flinkdb".
	 */
	public DbBackendConfig(String dbUserName, String dbUserPassword, String dbUrl) {
		this(dbUserName, dbUserPassword, Lists.newArrayList(dbUrl));
	}

	/**
	 * The username used to connect to the database at the given urls.
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * The password used to connect to the database at the given url and
	 * username.
	 */
	public String getUserPassword() {
		return userPassword;
	}

	/**
	 * Number of database shards defined.
	 */
	public int getNumberOfShards() {
		return shardUrls.size();
	}

	/**
	 * Database shard urls as provided in the constructor.
	 * 
	 */
	public List<String> getShardUrls() {
		return shardUrls;
	}

	/**
	 * The url of the first shard.
	 * 
	 */
	public String getUrl() {
		return getShardUrl(0);
	}

	/**
	 * The url of a specific shard.
	 * 
	 */
	public String getShardUrl(int shardIndex) {
		validateShardIndex(shardIndex);
		return shardUrls.get(shardIndex);
	}

	private void validateShardIndex(int i) {
		if (i < 0) {
			throw new IllegalArgumentException("Index must be positive.");
		} else if (getNumberOfShards() <= i) {
			throw new IllegalArgumentException("Index must be less then the total number of shards.");
		}
	}

	/**
	 * Get the {@link DbAdapter} that will be used to operate on the database
	 * during checkpointing.
	 * 
	 */
	public DbAdapter getDbAdapter() {
		return dbAdapter;
	}

	/**
	 * Set the {@link DbAdapter} that will be used to operate on the database
	 * during checkpointing.
	 * 
	 */
	public void setDbAdapter(DbAdapter adapter) {
		this.dbAdapter = adapter;
	}

	/**
	 * The class name that should be used to load the JDBC driver using
	 * Class.forName(JDBCDriverClass).
	 */
	public String getJDBCDriver() {
		return JDBCDriver;
	}

	/**
	 * Set the class name that should be used to load the JDBC driver using
	 * Class.forName(JDBCDriverClass).
	 */
	public void setJDBCDriver(String jDBCDriverClassName) {
		JDBCDriver = jDBCDriverClassName;
	}

	/**
	 * The maximum number of key-value pairs inserted in the database as one
	 * batch operation.
	 */
	public int getMaxKvInsertBatchSize() {
		return maxKvInsertBatchSize;
	}

	/**
	 * Set the maximum number of key-value pairs inserted in the database as one
	 * batch operation.
	 */
	public void setMaxKvInsertBatchSize(int size) {
		maxKvInsertBatchSize = size;
	}

	/**
	 * The number of times each SQL command will be retried on failure.
	 */
	public int getMaxNumberOfSqlRetries() {
		return maxNumberOfSqlRetries;
	}

	/**
	 * Sets the number of times each SQL command will be retried on failure.
	 */
	public void setMaxNumberOfSqlRetries(int maxNumberOfSqlRetries) {
		this.maxNumberOfSqlRetries = maxNumberOfSqlRetries;
	}

	/**
	 * The number of milliseconds slept between two SQL retries. The actual
	 * sleep time will be chosen randomly between 1 and the given time.
	 * 
	 */
	public int getSleepBetweenSqlRetries() {
		return sleepBetweenSqlRetries;
	}

	/**
	 * Sets the number of milliseconds slept between two SQL retries. The actual
	 * sleep time will be chosen randomly between 1 and the given time.
	 * 
	 */
	public void setSleepBetweenSqlRetries(int sleepBetweenSqlRetries) {
		this.sleepBetweenSqlRetries = sleepBetweenSqlRetries;
	}

	/**
	 * Sets the partitioner used to assign keys to different database shards
	 */
	public void setPartitioner(Partitioner partitioner) {
		this.shardPartitioner = partitioner;
	}

	/**
	 * Creates a new {@link ShardedConnection} using the set parameters.
	 * 
	 * @throws SQLException
	 */
	public ShardedConnection createShardedConnection() throws SQLException {
		if (JDBCDriver != null) {
			try {
				Class.forName(JDBCDriver);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Could not load JDBC driver class", e);
			}
		}
		if (shardPartitioner == null) {
			return new ShardedConnection(shardUrls, userName, userPassword);
		} else {
			return new ShardedConnection(shardUrls, userName, userPassword, shardPartitioner);
		}
	}
}
