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
import redis.clients.jedis.Protocol;

import java.io.Serializable;

/**
 * Configuration for Jedis Pool.
 */
public class JedisPoolConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private String host;
	private int port;
	private int timeout;
	private int database;
	private String password;
	private int maxTotal;
	private int maxIdle;
	private int minIdle;

	/**
	 * The host is mandatory, and when host is not set, it throws NullPointerException.
	 *
	 * @param host host hostname or IP
	 * @param port port port
	 * @param timeout timeout socket / connection timeout
	 * @param password password password, if any
	 * @param database database database index
	 * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool
	 * @param maxIdle the cap on the number of "idle" instances in the pool
     * @param minIdle the minimum number of idle objects to maintain in the pool
	 * @throws NullPointerException if do not see host
     */
	private JedisPoolConfig(String host, int port, int timeout, String password, int database,
							int maxTotal, int maxIdle, int minIdle) {
		Preconditions.checkNotNull(host, "Host information should be presented");
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		this.database = database;
		this.password = password;
		this.maxTotal = maxTotal;
		this.maxIdle = maxIdle;
		this.minIdle = minIdle;
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
	 * Returns host.
	 *
	 * @return hostname or IP
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Returns port.
	 *
	 * @return port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Returns timeout.
	 *
	 * @return socket / connection timeout
	 */
	public int getTimeout() {
		return timeout;
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
	 * Returns password.
	 *
	 * @return password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Builder for initializing JedisPoolConfig.
	 */
	public static class Builder {
		private String host;
		private int port = Protocol.DEFAULT_PORT;
		private int timeout = Protocol.DEFAULT_TIMEOUT;
		private int database = Protocol.DEFAULT_DATABASE;
		private String password;
		private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
		private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
		private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

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
		 * Sets host.
		 *
		 * @param host host
		 * @return Builder itself
		 */
		public Builder setHost(String host) {
			this.host = host;
			return this;
		}

		/**
		 * Sets port.
		 *
		 * @param port port
		 * @return Builder itself
		 */
		public Builder setPort(int port) {
			this.port = port;
			return this;
		}

		/**
		 * Sets timeout.
		 *
		 * @param timeout timeout
		 * @return Builder itself
		 */
		public Builder setTimeout(int timeout) {
			this.timeout = timeout;
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
		 * Builds JedisPoolConfig.
		 *
		 * @return JedisPoolConfig
		 */
		public JedisPoolConfig build() {
			return new JedisPoolConfig(host, port, timeout, password, database, maxTotal, maxIdle, minIdle);
		}
	}

	@Override
	public String toString() {
		return "JedisPoolConfig{" +
			"host='" + host + '\'' +
			", port=" + port +
			", timeout=" + timeout +
			", database=" + database +
			", maxTotal=" + maxTotal +
			", maxIdle=" + maxIdle +
			", minIdle=" + minIdle +
			'}';
	}
}
