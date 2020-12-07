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
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * Partition writer to write records with partition.
 *
 * <p>See {@link SingleDirectoryWriter}.
 * See {@link DynamicPartitionWriter}.
 * See {@link GroupedPartitionWriter}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public interface PartitionWriter<T> {

	/**
	 * Write a record.
	 */
	void write(T in) throws Exception;

	/**
	 * End a transaction.
	 */
	void close() throws Exception;

	/**
	 * Context for partition writer, provide some information and generation utils.
	 */
	class Context<T> {

		private final Configuration conf;
		private final OutputFormatFactory<T> factory;

		public Context(Configuration conf, OutputFormatFactory<T> factory) {
			this.conf = conf;
			this.factory = factory;
		}

		/**
		 * Create a new output format with path, configure it and open it.
		 */
		OutputFormat<T> createNewOutputFormat(Path path) throws IOException {
			OutputFormat<T> format = factory.createOutputFormat(path);
			format.configure(conf);
			// Here we just think of it as a single file format, so there can only be a single task.
			format.open(0, 1);
			return format;
		}
	}
}
