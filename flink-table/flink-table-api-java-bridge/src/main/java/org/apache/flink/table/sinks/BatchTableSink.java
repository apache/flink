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

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.table.api.Table;

/**
 * Defines an external {@link TableSink} to emit a batch {@link Table}.
 *
 * @param <T> Type of {@link DataSet} that this {@link TableSink} expects and supports.
 * @deprecated use {@link OutputFormatTableSink} instead.
 */
@Deprecated
public interface BatchTableSink<T> extends TableSink<T> {

    /**
     * Consumes the DataSet and return the {@link DataSink}. The returned {@link DataSink} will be
     * used to generate {@link Plan}.
     */
    DataSink<?> consumeDataSet(DataSet<T> dataSet);
}
