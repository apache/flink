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
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;

import java.util.Map;

/**
 * A factory to create configured table sink instances in a streaming environment based on
 * string-based properties. See also {@link TableSinkFactory} for more information.
 *
 * @param <T> type of records that the factory consumes
 */
@PublicEvolving
public interface StreamTableSinkFactory<T> extends TableSinkFactory<T> {

	/**
	 * Creates and configures a {@link StreamTableSink} using the given properties.
	 *
	 * @param properties normalized properties describing a table sink.
	 * @return the configured table sink.
	 */
	StreamTableSink<T> createStreamTableSink(Map<String, String> properties);

	/**
	 * Only create stream table sink.
	 */
	@Override
	default TableSink<T> createTableSink(Map<String, String> properties) {
		return createStreamTableSink(properties);
	}
}
