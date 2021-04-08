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

package org.apache.flink.streaming.connectors.redis.util;

import org.apache.flink.streaming.connectors.redis.config.RedisConfigConstants;

import redis.clients.jedis.Jedis;

import java.util.Properties;

/** */
public class JedisUtils {

    public static Jedis createResource(Properties configProps) {

        String host =
                configProps.getProperty(
                        RedisConfigConstants.REDIS_HOST, RedisConfigConstants.DEFAULT_REDIS_HOST);

        int port =
                Integer.parseInt(
                        configProps.getProperty(
                                RedisConfigConstants.REDIS_PORT,
                                Integer.toString(RedisConfigConstants.DEFAULT_REDIS_PORT)));

        return new Jedis(host, port);
    }
}
