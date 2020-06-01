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

package org.apache.flink.table.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;

/**
 * A {@link Format} for a {@link DynamicTableSink} for writing rows.
 *
 * @param <I> runtime interface needed by the table sink
 */
@PublicEvolving
public interface EncodingFormat<I> extends Format {

	/**
	 * Creates runtime encoder implementation that is configured to consume data of the given data type.
	 */
	I createRuntimeEncoder(DynamicTableSink.Context context, DataType consumedDataType);
}
