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
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.TableFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * File system format factory for creating configured instances of reader and writer.
 */
@Internal
public interface FileSystemFormatFactory extends TableFormatFactory<BaseRow> {

	/**
	 * Create {@link InputFormat} reader.
	 */
	InputFormat<BaseRow, ?> createReader(ReaderContext context);

	/**
	 * Create {@link Encoder} writer.
	 */
	Optional<Encoder<BaseRow>> createEncoder(WriterContext context);

	/**
	 * Create {@link BulkWriter.Factory} writer.
	 */
	Optional<BulkWriter.Factory<BaseRow>> createBulkWriterFactory(WriterContext context);

	/**
	 * Context of {@link #createReader}.
	 */
	interface ReaderContext {

		/**
		 * Full schema of the table.
		 */
		TableSchema getSchema();

		/**
		 * Properties of this format.
		 */
		Map<String, String> getFormatProperties();

		/**
		 * Partition keys of the table.
		 */
		List<String> getPartitionKeys();

		/**
		 * The default partition name in case the dynamic partition column value is
		 * null/empty string.
		 */
		String getDefaultPartName();

		/**
		 * Read paths.
		 */
		Path[] getPaths();

		/**
		 * Project the fields of the reader returned.
		 */
		int[] getProjectFields();

		/**
		 * Limiting push-down to reader. Reader only needs to try its best to limit the number
		 * of output records, but does not need to guarantee that the number must be less than or
		 * equal to the limit.
		 */
		long getPushedDownLimit();

		/**
		 * Pushed down filters, reader can try its best to filter records.
		 * The follow up operator will filter the records again.
		 */
		List<Expression> getPushedDownFilters();
	}

	/**
	 * Context of {@link #createEncoder} and {@link #createBulkWriterFactory}.
	 */
	interface WriterContext {

		/**
		 * Full schema of the table.
		 */
		TableSchema getSchema();

		/**
		 * Properties of this format.
		 */
		Map<String, String> getFormatProperties();

		/**
		 * Partition keys of the table.
		 */
		List<String> getPartitionKeys();

		/**
		 * Get field names without partition keys.
		 */
		default String[] getFieldNamesWithoutPartKeys() {
			return Arrays.stream(getSchema().getFieldNames())
					.filter(name -> !getPartitionKeys().contains(name))
					.toArray(String[]::new);
		}

		/**
		 * Get field types without partition keys.
		 */
		default DataType[] getFieldTypesWithoutPartKeys() {
			return Arrays.stream(getSchema().getFieldNames())
					.filter(name -> !getPartitionKeys().contains(name))
					.map(name -> getSchema().getFieldDataType(name).get())
					.toArray(DataType[]::new);
		}
	}
}
