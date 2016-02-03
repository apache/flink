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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

public class RedisSink<IN>  extends RichSinkFunction<IN> {

	private static final int DEFAULT_TIMEOUT = 2000;

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RedisSink.class);

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


	public RedisSink(String host, int port,  String channel, SerializationSchema<IN> schema) {
		this(host, port, channel, schema, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null);
	}

	public RedisSink(String host, int port, String channel, SerializationSchema<IN> schema, int timeOut, int soTimeOut, String password, int database, String clientName) {
		this(host, port, channel, schema, timeOut, soTimeOut, password, database, clientName, null);
	}

	public RedisSink(String host, int port, String channel, SerializationSchema<IN> schema, int timeOut, int soTimeOut, String password, int database, String clientName, JedisPoolConfig poolConfig) {
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


	@Override
	public void invoke(IN value) throws Exception {
		try (Jedis jedis = jedisPool.getResource()) {
			byte[] msg = schema.serialize(value);
			jedis.publish(channel.getBytes(), msg);
		} catch (Exception e) {
			if (logger.isErrorEnabled()) {
				logger.error("Cannot send Redis message {}", channel);
			}
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		if (this.poolConfig == null){
			this.poolConfig = new JedisPoolConfig();
		}
		this.jedisPool = new JedisPool(poolConfig, host, port, timeOut, soTimeOut, password, database, clientName);
	}

	@Override
	public void close() throws Exception {
		if (!jedisPool.isClosed()){
		try {
			jedisPool.close();
		} catch (JedisException e) {
			if (logger.isErrorEnabled()) {
				logger.error("failed to close Jedis pool");
			}
			throw new RuntimeException("Error while closing Redis connection with " + channel, e);
			}
		}
	}
}
