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

package org.apache.flink.table.connector.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.data.RowData;

/**
 * Provider of an {@link OutputFormat} instance as a runtime implementation for {@link DynamicTableSink}.
 */
@PublicEvolving
public interface OutputFormatProvider extends DynamicTableSink.SinkRuntimeProvider, ParallelismProvider {

	/**
	 * Helper method for creating a static provider.
	 */
	static OutputFormatProvider of(OutputFormat<RowData> outputFormat) {
		return () -> outputFormat;
	}

	/**
	 * Creates an {@link OutputFormat} instance.
	 */
	OutputFormat<RowData> createOutputFormat();
}
