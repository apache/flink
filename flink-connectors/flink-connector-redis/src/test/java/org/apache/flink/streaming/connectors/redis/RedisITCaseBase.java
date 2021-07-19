/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.streaming.connectors.redis.config.RedisConfigConstants;
import org.apache.flink.streaming.connectors.redis.util.JedisUtils;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.After;
import org.junit.Before;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** */
public abstract class RedisITCaseBase extends AbstractTestBase {

    public static final String REDIS_IMAGE = "redis";
    private static final int REDIS_PORT = 6379;

    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static final GenericContainer<?> container =
            new GenericContainer<>(REDIS_IMAGE).withExposedPorts(REDIS_PORT);

    protected Jedis jedis;

    protected static synchronized void start() {
        if (!running.get()) {
            container.start();
            running.set(true);
        }
    }

    protected static void stop() {
        container.stop();
        running.set(false);
    }

    @Before
    public void setUp() {
        jedis = JedisUtils.createResource(getConfigProperties());
        jedis.flushAll();
    }

    @After
    public void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
    }

    protected Properties getConfigProperties() {
        start();

        Properties configProps = new Properties();
        configProps.setProperty(RedisConfigConstants.REDIS_HOST, container.getContainerIpAddress());
        configProps.setProperty(
                RedisConfigConstants.REDIS_PORT, Integer.toString(container.getFirstMappedPort()));
        return configProps;
    }
}
