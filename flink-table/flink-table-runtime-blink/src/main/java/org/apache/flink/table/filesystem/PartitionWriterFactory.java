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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.filesystem.PartitionWriter.Context;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * Factory of {@link PartitionWriter} to avoid virtual function calls.
 */
@Internal
public interface PartitionWriterFactory<T> extends Serializable {

	PartitionWriter<T> create(
			Context<T> context,
			PartitionTempFileManager manager,
			PartitionComputer<T> computer) throws Exception;

	/**
	 * Util for get a {@link PartitionWriterFactory}.
	 */
	static <T> PartitionWriterFactory<T> get(
			boolean dynamicPartition,
			boolean grouped,
			LinkedHashMap<String, String> staticPartitions) {
		if (dynamicPartition) {
			return grouped ? GroupedPartitionWriter::new : DynamicPartitionWriter::new;
		} else {
			return (PartitionWriterFactory<T>) (context, manager, computer) ->
					new SingleDirectoryWriter<>(context, manager, computer, staticPartitions);
		}
	}
}
