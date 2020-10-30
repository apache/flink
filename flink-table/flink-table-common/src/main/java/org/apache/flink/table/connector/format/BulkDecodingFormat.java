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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.List;

/**
 * A {@link Format} for a {@link DynamicTableSource} for reading rows by {@link BulkFormat}.
 */
@Internal
public interface BulkDecodingFormat<T> extends DecodingFormat<BulkFormat<T, FileSourceSplit>> {

	/**
	 * Provides a list of filters in conjunctive form  for filtering on a best-effort basis.
	 */
	default void applyFilters(List<ResolvedExpression> filters) {}
}
