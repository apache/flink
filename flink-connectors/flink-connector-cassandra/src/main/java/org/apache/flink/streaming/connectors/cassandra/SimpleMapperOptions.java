/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.mapping.Mapper;

import java.util.ArrayList;

/** A simple MapperOptions implementation. */
public class SimpleMapperOptions implements MapperOptions {

    private static final long serialVersionUID = 1L;

    private final ArrayList<Mapper.Option> options;

    public SimpleMapperOptions() {
        options = new ArrayList<>();
    }

    /**
     * Adds time-to-live option to a mapper operation. This is only valid for save operations.
     *
     * <p>Note that this option is only available if using {@link ProtocolVersion#V2} or above.
     *
     * @param ttl the TTL (in seconds).
     */
    public SimpleMapperOptions ttl(int ttl) {
        options.add(Mapper.Option.ttl(ttl));
        return this;
    }

    /**
     * Adds a timestamp option to a mapper operation. This is only valid for save and delete
     * operations.
     *
     * <p>Note that this option is only available if using {@link ProtocolVersion#V2} or above.
     *
     * @param timestamp the timestamp (in microseconds).
     */
    public SimpleMapperOptions timestamp(long timestamp) {
        options.add(Mapper.Option.timestamp(timestamp));
        return this;
    }

    /**
     * Adds a consistency level value option to a mapper operation. This is valid for save, delete
     * and get operations.
     *
     * <p>Note that the consistency level can also be defined at the mapper level, as a parameter of
     * the {@link com.datastax.driver.mapping.annotations.Table} annotation (this is redundant for
     * backward compatibility). This option, whether defined on a specific call or as the default,
     * will always take precedence over the annotation.
     *
     * @param cl the {@link com.datastax.driver.core.ConsistencyLevel} to use for the operation.
     */
    public SimpleMapperOptions consistencyLevel(ConsistencyLevel cl) {
        options.add(Mapper.Option.consistencyLevel(cl));
        return this;
    }

    /**
     * Enables query tracing for a mapper operation. This is valid for save, delete and get
     * operations.
     *
     * @param enabled whether to enable tracing.
     */
    public SimpleMapperOptions tracing(boolean enabled) {
        options.add(Mapper.Option.tracing(enabled));
        return this;
    }

    /**
     * Specifies whether null entity fields should be included in insert queries. This option is
     * valid only for save operations.
     *
     * <p>If this option is not specified, it defaults to {@code true} (null fields are saved).
     *
     * @param enabled whether to include null fields in queries.
     */
    public SimpleMapperOptions saveNullFields(boolean enabled) {
        options.add(Mapper.Option.saveNullFields(enabled));
        return this;
    }

    /**
     * Specifies whether an IF NOT EXISTS clause should be included in insert queries. This option
     * is valid only for save operations.
     *
     * <p>If this option is not specified, it defaults to {@code false} (IF NOT EXISTS statements
     * are not used).
     *
     * @param enabled whether to include an IF NOT EXISTS clause in queries.
     */
    public SimpleMapperOptions ifNotExists(boolean enabled) {
        options.add(Mapper.Option.ifNotExists(enabled));
        return this;
    }

    @Override
    public Mapper.Option[] getMapperOptions() {
        return options.toArray(new Mapper.Option[0]);
    }
}
