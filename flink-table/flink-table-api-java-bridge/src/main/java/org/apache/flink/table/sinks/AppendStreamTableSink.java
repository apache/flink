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

package org.apache.flink.table.sinks;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * Defines an external {@link TableSink} to emit streaming {@link Table} with only insert changes.
 *
 * <p>If the {@link Table} is also modified by update or delete changes, a {@link TableException}
 * will be thrown.
 *
 * @param <T> Type of {@link DataStream} that this {@link TableSink} expects and supports.
 *
 * @deprecated This interface has been replaced by {@link DynamicTableSink}. The new interface consumes
 *             internal data structures and only works with the Blink planner. See FLIP-95 for more
 *             information.
 */
@Deprecated
@PublicEvolving
public interface AppendStreamTableSink<T> extends StreamTableSink<T> {

}
