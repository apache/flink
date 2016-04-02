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
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * Configuration for Jedis Cluster.
 */
public class JedisClusterConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	private Set<InetSocketAddress> nodes;
	private int timeout;
	private int maxRedirections;
	private int maxTotal;
	private int maxIdle;
	private int minIdle;

	/**
	 * The list of node is mandatory, and when nodes is not set, it throws NullPointerException.
	 *
	 * @param nodes nodes list of node information for JedisCluster
	 * @param timeout timeout socket / connection timeout
	 * @param maxRedirections maxRedirections limit of redirections - how much we'll follow MOVED or ASK
	 * @param maxTotal the maximum number of objects that can be allocated by the pool
	 * @param maxIdle the cap on the number of "idle" instances in the pool
     * @param minIdle the minimum number of idle objects to maintain in the pool
	 * @throws NullPointerException if nodes are null
	 */
	private JedisClusterConfig(Set<InetSocketAddress> nodes, int timeout, int maxRedirections,
								int maxTotal, int maxIdle, int minIdle) {

		Preconditions.checkNotNull(nodes, "Node information should be presented");

		this.nodes = nodes;
		this.timeout = timeout;
		this.maxRedirections = maxRedirections;
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
	 * Returns nodes.
	 *
	 * @return list of node information
	 */
	public Set<HostAndPort> getNodes() {
		Set<HostAndPort> ret = new HashSet<>();
		for (InetSocketAddress node : nodes) {
			ret.add(new HostAndPort(node.getHostName(), node.getPort()));
		}
		return ret;
	}

	/**
	 * Returns socket / connection timeout.
	 *
	 * @return socket / connection timeout
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * Returns limit of redirection.
	 *
	 * @return limit of redirection
	 */
	public int getMaxRedirections() {
		return maxRedirections;
	}


	/**
	 * Builder for initializing JedisClusterConfig.
	 */
	public static class Builder {
		private Set<InetSocketAddress> nodes;
		private int timeout = Protocol.DEFAULT_TIMEOUT;
		private int maxRedirections = 5;
		private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
		private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
		private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

		/**
		 * Sets list of node.
		 *
		 * @param nodes list of node
		 * @return Builder itself
		 */
		public Builder setNodes(Set<InetSocketAddress> nodes) {
			this.nodes = nodes;
			return this;
		}

		/**
		 * Sets socket / connection timeout.
		 *
		 * @param timeout socket / connection timeout
		 * @return Builder itself
		 */
		public Builder setTimeout(int timeout) {
			this.timeout = timeout;
			return this;
		}

		/**
		 * Sets limit of redirection.
		 *
		 * @param maxRedirections limit of redirection
		 * @return Builder itself
		 */
		public Builder setMaxRedirections(int maxRedirections) {
			this.maxRedirections = maxRedirections;
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
		 * Builds JedisClusterConfig.
		 *
		 * @return JedisClusterConfig
		 */
		public JedisClusterConfig build() {
			return new JedisClusterConfig(nodes, timeout, maxRedirections, maxTotal, maxIdle, minIdle);
		}
	}

	@Override
	public String toString() {
		return "JedisClusterConfig{" +
			"nodes=" + nodes +
			", timeout=" + timeout +
			", maxRedirections=" + maxRedirections +
			", maxTotal=" + maxTotal +
			", maxIdle=" + maxIdle +
			", minIdle=" + minIdle +
			'}';
	}
}
