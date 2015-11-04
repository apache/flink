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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * 
 * Configuration object for {@link DbStateBackend}, containing information to
 * shard and connect to the databases that will store the state checkpoints.
 *
 */
public class DbBackendConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	// Database connection properties
	private final String userName;
	private final String userPassword;
	private final List<String> shardUrls;

	// JDBC Driver + DbAdapter information
	private Class<? extends MySqlAdapter> dbAdapterClass = MySqlAdapter.class;
	private String JDBCDriver = null;

	private int maxNumberOfSqlRetries = 5;
	private int sleepBetweenSqlRetries = 100;

	// KvState properties
	private int kvStateCacheSize = 10000;
	private int maxKvInsertBatchSize = 1000;
	private float maxKvEvictFraction = 0.1f;
	private int kvStateCompactionFreq = -1;

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

	/**
	 * Get an instance of the {@link MySqlAdapter} that will be used to operate on
	 * the database during checkpointing.
	 * 
	 * @return An instance of the class set in {@link #setDbAdapterClass(Class)}
	 *         or a {@link MySqlAdapter} instance if a custom class was not set.
	 */
	public MySqlAdapter getDbAdapter() {
		try {
			return dbAdapterClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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
	 * Get the Class that will be used to instantiate the {@link MySqlAdapter} for
	 * the {@link #getDbAdapter()} method.
	 * 
	 */
	public Class<? extends MySqlAdapter> getDbAdapterClass() {
		return dbAdapterClass;
	}

	/**
	 * Set the Class that will be used to instantiate the {@link MySqlAdapter} for
	 * the {@link #getDbAdapter()} method. The class should have an empty
	 * constructor.
	 * 
	 */
	public void setDbAdapterClass(Class<? extends MySqlAdapter> dbAdapterClass) {
		this.dbAdapterClass = dbAdapterClass;
	}

	/**
	 * The maximum number of key-value pairs stored in one task instance's cache
	 * before evicting to the underlying database.
	 *
	 */
	public int getKvCacheSize() {
		return kvStateCacheSize;
	}

	/**
	 * Set the maximum number of key-value pairs stored in one task instance's
	 * cache before evicting to the underlying database. When the cache is full
	 * the N least recently used keys will be evicted to the database, where N =
	 * maxKvEvictFraction*KvCacheSize.
	 *
	 */
	public void setKvCacheSize(int size) {
		kvStateCacheSize = size;
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
	 * Sets the maximum fraction of key-value states evicted from the cache if
	 * the cache is full.
	 */
	public void setMaxKvCacheEvictFraction(float fraction) {
		if (fraction > 1 || fraction <= 0) {
			throw new RuntimeException("Must be a number between 0 and 1");
		} else {
			maxKvEvictFraction = fraction;
		}
	}

	/**
	 * The maximum fraction of key-value states evicted from the cache if the
	 * cache is full.
	 */
	public float getMaxKvCacheEvictFraction() {
		return maxKvEvictFraction;
	}

	/**
	 * The number of elements that will be evicted when the cache is full.
	 * 
	 */
	public int getNumElementsToEvict() {
		return (int) Math.ceil(getKvCacheSize() * getMaxKvCacheEvictFraction());
	}

	/**
	 * Sets how often will automatic compaction be performed on the database to
	 * remove old overwritten state changes. The frequency is set in terms of
	 * number of successful checkpoints between two compactions and should take
	 * the state size and checkpoint frequency into account.
	 * <p>
	 * By default automatic compaction is turned off.
	 */
	public void setKvStateCompactionFrequency(int compactEvery) {
		this.kvStateCompactionFreq = compactEvery;
	}

	/**
	 * Sets how often will automatic compaction be performed on the database to
	 * remove old overwritten state changes. The frequency is set in terms of
	 * number of successful checkpoints between two compactions and should take
	 * the state size and checkpoint frequency into account.
	 * <p>
	 * By default automatic compaction is turned off.
	 */
	public int getKvStateCompactionFrequency() {
		return kvStateCompactionFreq;
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
	 * Creates a new {@link Connection} using the set parameters for the first
	 * shard.
	 * 
	 * @throws SQLException
	 */
	public Connection createConnection() throws SQLException {
		return createConnection(0);
	}

	/**
	 * Creates a new {@link Connection} using the set parameters for the given
	 * shard index.
	 * 
	 * @throws SQLException
	 */
	public Connection createConnection(int shardIndex) throws SQLException {
		validateShardIndex(shardIndex);
		if (JDBCDriver != null) {
			try {
				Class.forName(JDBCDriver);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Could not load JDBC driver class", e);
			}
		}
		return DriverManager.getConnection(getShardUrl(shardIndex), userName, userPassword);
	}

	/**
	 * Creates a new {@link DbBackendConfig} with the selected shard as its only
	 * shard.
	 * 
	 */
	public DbBackendConfig createConfigForShard(int shardIndex) {
		validateShardIndex(shardIndex);
		DbBackendConfig c = new DbBackendConfig(userName, userPassword, shardUrls.get(shardIndex));
		c.setJDBCDriver(JDBCDriver);
		c.setDbAdapterClass(dbAdapterClass);
		c.setKvCacheSize(kvStateCacheSize);
		c.setMaxKvInsertBatchSize(maxKvInsertBatchSize);
		return c;
	}

	private void validateShardIndex(int i) {
		if (i < 0) {
			throw new IllegalArgumentException("Index must be positive.");
		} else if (getNumberOfShards() <= i) {
			throw new IllegalArgumentException("Index must be less then the total number of shards.");
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((JDBCDriver == null) ? 0 : JDBCDriver.hashCode());
		result = prime * result + ((dbAdapterClass == null) ? 0 : dbAdapterClass.hashCode());
		result = prime * result + kvStateCacheSize;
		result = prime * result + Float.floatToIntBits(maxKvEvictFraction);
		result = prime * result + maxKvInsertBatchSize;
		result = prime * result + kvStateCompactionFreq;
		result = prime * result + ((shardUrls == null) ? 0 : shardUrls.hashCode());
		result = prime * result + ((userName == null) ? 0 : userName.hashCode());
		result = prime * result + ((userPassword == null) ? 0 : userPassword.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		DbBackendConfig other = (DbBackendConfig) obj;
		if (JDBCDriver == null) {
			if (other.JDBCDriver != null) {
				return false;
			}
		} else if (!JDBCDriver.equals(other.JDBCDriver)) {
			return false;
		}
		if (dbAdapterClass == null) {
			if (other.dbAdapterClass != null) {
				return false;
			}
		} else if (!dbAdapterClass.equals(other.dbAdapterClass)) {
			return false;
		}
		if (kvStateCacheSize != other.kvStateCacheSize) {
			return false;
		}
		if (Float.floatToIntBits(maxKvEvictFraction) != Float.floatToIntBits(other.maxKvEvictFraction)) {
			return false;
		}
		if (maxKvInsertBatchSize != other.maxKvInsertBatchSize) {
			return false;
		}
		if (kvStateCompactionFreq != other.kvStateCompactionFreq) {
			return false;
		}
		if (shardUrls == null) {
			if (other.shardUrls != null) {
				return false;
			}
		} else if (!shardUrls.equals(other.shardUrls)) {
			return false;
		}
		if (userName == null) {
			if (other.userName != null) {
				return false;
			}
		} else if (!userName.equals(other.userName)) {
			return false;
		}
		if (userPassword == null) {
			if (other.userPassword != null) {
				return false;
			}
		} else if (!userPassword.equals(other.userPassword)) {
			return false;
		}
		return true;
	}

}
