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
package org.apache.flink.streaming.connectors.redis.common.config;

import com.google.common.base.Preconditions;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Protocol;

import java.io.Serializable;
import java.util.Set;

/**
 * Configuration for Jedis Sentinel Pool.
 */
public class JedisSentinelConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JedisSentinelConfig.class);

	private String masterName;
	private Set<String> sentinels;
	private int connectionTimeout;
	private int soTimeout;
	private String password;
	private int database;
	private int maxTotal;
	private int maxIdle;
	private int minIdle;

	/**
	 * The master name and sentinels are mandatory, and when you didn't set these, it throws NullPointerException.
	 *
	 * @param masterName master name of the replica set
	 * @param sentinels set of sentinel hosts
	 * @param connectionTimeout timeout connection timeout
	 * @param soTimeout timeout socket timeout
	 * @param password password password, if any
	 * @param database database database index
	 * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool
	 * @param maxIdle the cap on the number of "idle" instances in the pool
	 * @param minIdle the minimum number of idle objects to maintain in the pool
	 *
	 * @throws NullPointerException if do not see master name or sentinels
	 * @throws IllegalArgumentException if sentinels are empty
	 */
	private JedisSentinelConfig(String masterName, Set<String> sentinels,
								int connectionTimeout, int soTimeout,
								String password, int database,
								int maxTotal, int maxIdle, int minIdle) {
		Preconditions.checkNotNull(masterName, "Master name should be presented");
		Preconditions.checkNotNull(sentinels, "Sentinels information should be presented");
		Preconditions.checkArgument(!sentinels.isEmpty(), "Sentinel hosts should not be empty");

		this.masterName = masterName;
		this.sentinels = sentinels;
		this.connectionTimeout = connectionTimeout;
		this.soTimeout = soTimeout;
		this.password = password;
		this.database = database;
		this.maxTotal = maxTotal;
		this.maxIdle = maxIdle;
		this.minIdle = minIdle;
	}

	/**
	 * Returns master name of the replica set.
	 *
	 * @return master name of the replica set.
	 */
	public String getMasterName() {
		return masterName;
	}

	/**
	 * Returns Sentinels host addresses.
	 *
	 * @return Set of Sentinels host addresses
	 */
	public Set<String> getSentinels() {
		return sentinels;
	}

	/**
	 * Returns timeout.
	 *
	 * @return connection timeout
	 */
	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	/**
	 * Returns socket timeout.
	 *
	 * @return socket timeout
	 */
	public int getSoTimeout() {
		return soTimeout;
	}

	/**
	 * Returns password.
	 *
	 * @return password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Returns database index.
	 *
	 * @return database index
	 */
	public int getDatabase() {
		return database;
	}

	/**
	 * Get the value for the {@code maxTotal} configuration attribute
	 * for pools to be created with this configuration instance.
	 *
	 * @return  The current setting of {@code maxTotal} for this
	 *          configuration instance
	 * @see GenericObjectPoolConfig#getMaxTotal()
	 */
	public int getMaxTotal() {
		return maxTotal;
	}

	/**
	 * Get the value for the {@code maxIdle} configuration attribute
	 * for pools to be created with this configuration instance.
	 *
	 * @return  The current setting of {@code maxIdle} for this
	 *          configuration instance
	 * @see GenericObjectPoolConfig#getMaxIdle()
	 */
	public int getMaxIdle() {
		return maxIdle;
	}

	/**
	 * Get the value for the {@code minIdle} configuration attribute
	 * for pools to be created with this configuration instance.
	 *
	 * @return  The current setting of {@code minIdle} for this
	 *          configuration instance
	 * @see GenericObjectPoolConfig#getMinIdle()
	 */
	public int getMinIdle() {
		return minIdle;
	}

	/**
	 * Builder for initializing JedisSentinelConfig.
	 */
	public static class Builder {
		private String masterName;
		private Set<String> sentinels;
		private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
		private int soTimeout = Protocol.DEFAULT_TIMEOUT;
		private String password;
		private int database = Protocol.DEFAULT_DATABASE;
		private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
		private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
		private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

		/**
		 * Sets master name of the replica set.
		 *
		 * @param masterName  master name of the replica set
		 * @return Builder itself
         */
		public Builder setMasterName(String masterName) {
			this.masterName = masterName;
			return this;
		}

		/**
		 * Sets sentinels address.
		 *
		 * @param sentinels host set of the sentinels
		 * @return Builder itself
         */
		public Builder setSentinels(Set<String> sentinels) {
			this.sentinels = sentinels;
			return this;
		}

		/**
		 * Sets connection timeout.
		 *
		 * @param connectionTimeout connection timeout
		 * @return Builder itself
		 */
		public Builder setConnectionTimeout(int connectionTimeout) {
			this.connectionTimeout = connectionTimeout;
			return this;
		}

		/**
		 * Sets socket timeout.
		 *
		 * @param soTimeout socket timeout
         * @return Builder itself
         */
		public Builder setSoTimeout(int soTimeout) {
			this.soTimeout = soTimeout;
			return this;
		}

		/**
		 * Sets password.
		 *
		 * @param password password, if any
		 * @return Builder itself
		 */
		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		/**
		 * Sets database index.
		 *
		 * @param database database index
		 * @return Builder itself
		 */
		public Builder setDatabase(int database) {
			this.database = database;
			return this;
		}

		/**
		 * Sets value for the {@code maxTotal} configuration attribute
		 * for pools to be created with this configuration instance.
		 *
		 * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool
		 * @return Builder itself
		 */
		public Builder setMaxTotal(int maxTotal) {
			this.maxTotal = maxTotal;
			return this;
		}

		/**
		 * Sets value for the {@code maxIdle} configuration attribute
		 * for pools to be created with this configuration instance.
		 *
		 * @param maxIdle the cap on the number of "idle" instances in the pool
		 * @return Builder itself
		 */
		public Builder setMaxIdle(int maxIdle) {
			this.maxIdle = maxIdle;
			return this;
		}

		/**
		 * Sets value for the {@code minIdle} configuration attribute
		 * for pools to be created with this configuration instance.
		 *
		 * @param minIdle the minimum number of idle objects to maintain in the pool
		 * @return Builder itself
		 */
		public Builder setMinIdle(int minIdle) {
			this.minIdle = minIdle;
			return this;
		}

		/**
		 * Builds JedisSentinelConfig.
		 *
		 * @return JedisSentinelConfig
		 */
		public JedisSentinelConfig build(){
			return new JedisSentinelConfig(masterName, sentinels, connectionTimeout, soTimeout,
				password, database, maxTotal, maxIdle, minIdle);
		}
	}

	@Override
	public String toString() {
		return "JedisSentinelConfig{" +
			"masterName='" + masterName + '\'' +
			", connectionTimeout=" + connectionTimeout +
			", soTimeout=" + soTimeout +
			", database=" + database +
			", maxTotal=" + maxTotal +
			", maxIdle=" + maxIdle +
			", minIdle=" + minIdle +
			'}';
	}
}
