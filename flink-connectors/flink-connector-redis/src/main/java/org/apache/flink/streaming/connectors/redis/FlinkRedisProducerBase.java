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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import redis.clients.jedis.Jedis;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** @param <OUT> */
public abstract class FlinkRedisProducerBase<OUT> extends RichSinkFunction<OUT>
        implements CheckpointedFunction {

    private final Properties configProps;

    private transient Jedis jedis;

    private final String key;

    public FlinkRedisProducerBase(String key, Properties configProps) {
        checkNotNull(key, "key can not be null");
        this.key = key;

        checkNotNull(configProps, "configProps can not be null");
        this.configProps = configProps;
    }

    /** Initializes the connection to Redis. */
    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        // code
        createResource(this.configProps);
    }

    protected void createResource(Properties config) {
        this.jedis = new Jedis();
    }

    @Override
    public void close() throws Exception {
        super.close();
        // code
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    public void invoke(OUT value, Context context) throws Exception {
        invoke(this.jedis, this.key, value, context);
    }

    protected abstract void invoke(Jedis resource, String key, OUT value, Context context)
            throws Exception;

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        // in synchronous mode, nothing to do
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // nothing to do
    }
}
