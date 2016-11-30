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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Base class for Flink Redis configuration.
 */
public abstract class FlinkJedisConfigBase implements Serializable {
	private static final long serialVersionUID = 1L;

	protected final int maxTotal;
	protected final int maxIdle;
	protected final int minIdle;
	protected final int connectionTimeout;

	protected FlinkJedisConfigBase(int connectionTimeout, int maxTotal, int maxIdle, int minIdle){
		Preconditions.checkArgument(connectionTimeout >= 0, "connection timeout can not be negative");
		Preconditions.checkArgument(maxTotal >= 0, "maxTotal value can not be negative");
		Preconditions.checkArgument(maxIdle >= 0, "maxIdle value can not be negative");
		Preconditions.checkArgument(minIdle >= 0, "minIdle value can not be negative");
		this.connectionTimeout = connectionTimeout;
		this.maxTotal = maxTotal;
		this.maxIdle = maxIdle;
		this.minIdle = minIdle;
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
}
