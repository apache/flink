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
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** @param <T> */
public abstract class RedisConsumerBase<T> extends RichParallelSourceFunction<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(RedisConsumerBase.class);

    private final Properties configProps;

    private transient Jedis jedis;

    private final List<String> keys;

    /** Flag indicating whether the consumer is still running. */
    private volatile boolean running = true;

    public RedisConsumerBase(List<String> keys, Properties configProps) {
        checkNotNull(keys, "keys can not be null");
        checkArgument(keys.size() != 0, "must be consuming at least 1 key");
        this.keys = Collections.unmodifiableList(keys);

        this.configProps = checkNotNull(configProps, "configProps can not be null");

        if (LOG.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (String key : keys) {
                sb.append(key).append(", ");
            }
            LOG.info(
                    "Flink Redis Stream Consumer is going to read the following streams: {}",
                    sb.toString());
        }
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        // code
        super.open(configuration);
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        if (!running) {
            return;
        }
        this.jedis = createResource(this.configProps);

        while (running) {
            running = readAndCollect(this.jedis, this.keys, sourceContext);
        }
    }

    private static Jedis createResource(Properties configProps) {
        return new Jedis();
    }

    protected abstract boolean readAndCollect(
            Jedis jedis, List<String> keys, SourceContext<T> sourceContext);

    @Override
    public void cancel() {
        // set ourselves as not running;
        // this would let the main discovery loop escape as soon as possible
        running = false;

        // abort the fetcher, if there is one
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    public void close() throws Exception {
        cancel();

        super.close();
    }
}
