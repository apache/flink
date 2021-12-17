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

package org.apache.flink.table.sources;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.source.DynamicTableSource;

/**
 * Defines an external stream table and provides read access to its data.
 *
 * @param <T> Type of the {@link DataStream} created by this {@link TableSource}.
 * @deprecated This interface has been replaced by {@link DynamicTableSource}. The new interface
 *     produces internal data structures. See FLIP-95 for more information.
 */
@Deprecated
public interface StreamTableSource<T> extends TableSource<T> {

    /**
     * Returns true if this is a bounded source, false if this is an unbounded source. Default is
     * unbounded for compatibility.
     */
    default boolean isBounded() {
        return false;
    }

    /**
     * Returns the data of the table as a {@link DataStream}.
     *
     * <p>NOTE: This method is for internal use only for defining a {@link TableSource}. Do not use
     * it in Table API programs.
     */
    DataStream<T> getDataStream(StreamExecutionEnvironment execEnv);
}
