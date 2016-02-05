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
package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A sink that delivers data to a Redis channel using the Jedis client.
 *
 *  <p>
 * When creating the sink {@code host, port, schema} must be specified or else it will throw
 * {@link NullPointerException}
 *
 *  * <p>
 * Example:
 *
 * <pre>{@code
 *     new RedisSink<String>(host, port, schema)
 * }</pre>
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class RedisSink<IN> extends RichSinkFunction<IN> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

	private static final int DEFAULT_TIMEOUT = 2000;

	private String host;
	private int port;
	private String channel;
	private int timeOut;
	private int soTimeOut;
	private int database;
	private String password;
	private String clientName;

	private SerializationSchema<IN> schema;
	private transient JedisPoolConfig poolConfig;
	private transient JedisPool jedisPool;

	/**
	 * Creates a new RedisSink. For passing custom connection Pool config, please use the constructor
	 * {@link RedisSink#RedisSink(String, int, String, SerializationSchema, int, int, String, int, String,
	 * JedisPoolConfig poolConfig)},
	 * @param host Redis Host name to connect to
	 * @param port Redis instance Port
	 * @param channel The channel to which data will be published
	 * @param schema A {@link SerializationSchema} for turning the java object to bytes.
	 */
	public RedisSink(String host, int port,  String channel, SerializationSchema<IN> schema) {
		this(host, port, channel, schema, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null, null);
	}

	/**
	 * Creates a new RedisSink.
	 * @param host Redis Host name to connect to
	 * @param port Redis instance Port
	 * @param channel The channel to which data will be published
	 * @param schema A {@link SerializationSchema} for turning the java object to bytes.
	 * @param timeOut Connection Timeout in millisecond
	 * @param soTimeOut Socket Timeout in millisecond. Sets Socket::SO_TIMEOUT
	 * @param password Password for Redis Server
	 * @param database Select the DB with having the specified zero-based numeric index
	 * @param clientName Assigns a name to the current connection using CLIENT SETNAME command
	 * @param poolConfig Custom jedis connection pool configuration
	 */
	public RedisSink(String host, int port, String channel, SerializationSchema<IN> schema, int timeOut,
					int soTimeOut, String password, int database, String clientName, JedisPoolConfig poolConfig) {

		Preconditions.checkNotNull(host, "Redis host name should not be Null");
		Preconditions.checkNotNull(channel, "Redis Channel name can not be null");
		Preconditions.checkNotNull(schema, "SerializationSchema should not be Null");

		this.host = host;
		this.port = port;
		this.channel = channel;
		this.schema = schema;
		this.timeOut = timeOut;
		this.soTimeOut = soTimeOut;
		this.password = password;
		this.database = database;
		this.clientName = clientName;
		this.poolConfig = poolConfig;
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Redis channel.
	 *
	 * @param value The incoming data
	 */
	@Override
	public void invoke(IN value) throws Exception {
		try (Jedis jedis = jedisPool.getResource()) {
			byte[] msg = schema.serialize(value);
			jedis.publish(channel.getBytes(), msg);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message {}", channel);
			}
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {

		if (this.poolConfig == null){
			this.poolConfig = new JedisPoolConfig();
		}
		if (this.jedisPool == null){
			this.jedisPool = new JedisPool(poolConfig, host, port, timeOut, soTimeOut, password, database, clientName);
		}
	}

	/**
	 * Closes the jedis Connection Pool
	 * @throws Exception
	 */
	@Override
	public void close() throws Exception {
		if (!jedisPool.isClosed()){
			try {
				jedisPool.close();
			} catch (JedisException e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("failed to close Jedis pool");
				}
				throw new RuntimeException("Error while closing Redis connection with " + channel, e);
			}
		}
	}
}
