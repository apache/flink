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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;

import java.util.Map;

/**
 * A factory to create configured table sink instances in a streaming environment based on
 * string-based properties. See also {@link TableSinkFactory} for more information.
 *
 * @param <T> type of records that the factory consumes
 * @deprecated This interface has been replaced by {@link DynamicTableSinkFactory}. The new
 *     interface creates instances of {@link DynamicTableSink}. See FLIP-95 for more information.
 */
@Deprecated
@PublicEvolving
public interface StreamTableSinkFactory<T> extends TableSinkFactory<T> {

    /**
     * Creates and configures a {@link StreamTableSink} using the given properties.
     *
     * @param properties normalized properties describing a table sink.
     * @return the configured table sink.
     * @deprecated {@link Context} contains more information, and already contains table schema too.
     *     Please use {@link #createTableSink(Context)} instead.
     */
    @Deprecated
    default StreamTableSink<T> createStreamTableSink(Map<String, String> properties) {
        return null;
    }

    /** Only create stream table sink. */
    @Override
    default TableSink<T> createTableSink(Map<String, String> properties) {
        StreamTableSink<T> sink = createStreamTableSink(properties);
        if (sink == null) {
            throw new ValidationException("Please override 'createTableSink(Context)' method.");
        }
        return sink;
    }
}
