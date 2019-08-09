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

package org.apache.flink.table.catalog;

import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.StatisticsValidator.STATISTICS_COLUMNS;
import static org.apache.flink.table.descriptors.StatisticsValidator.STATISTICS_ROW_COUNT;
import static org.apache.flink.table.descriptors.StatisticsValidator.readColumnStats;

/**
 * Defines a table in an {@link ExternalCatalog}. External catalog tables describe table sources
 * and/or sinks for both batch and stream environments.
 *
 * <p>See also {@link TableFactory} for more information about how to target suitable factories.
 *
 * <p>Use {@code ExternalCatalogTableBuilder} to integrate with the normalized descriptor-based API.
 *
 * @deprecated use {@link CatalogTable} instead.
 */
@Deprecated
public class ExternalCatalogTable implements Descriptor {

	/**
	 * Flag whether this external table is intended for batch environments.
	 */
	private final boolean isBatch;

	/**
	 * Flag whether this external table is intended for streaming environments.
	 */
	private final boolean isStreaming;

	/**
	 * Flag whether this external table is declared as table source.
	 */
	private final boolean isSource;

	/**
	 * Flag whether this external table is declared as table sink.
	 */
	private final boolean isSink;

	/**
	 * Properties that describe the table and should match with a {@link TableFactory}.
	 */
	private final Map<String, String> properties;

	public ExternalCatalogTable(
		boolean isBatch,
		boolean isStreaming,
		boolean isSource,
		boolean isSink,
		Map<String, String> properties) {
		this.isBatch = isBatch;
		this.isStreaming = isStreaming;
		this.isSource = isSource;
		this.isSink = isSink;
		this.properties = properties;
	}

	// ----------------------------------------------------------------------------------------------
	// Legacy code
	// ---------------------------------------------------------------------------------------------

	/**
	 * Reads table statistics from the descriptors properties.
	 *
	 * @deprecated This method exists for backwards-compatibility only.
	 */
	@Deprecated
	public Optional<TableStats> getTableStats() {
		DescriptorProperties normalizedProps = new DescriptorProperties();
		normalizedProps.putProperties(normalizedProps);
		Optional<Long> rowCount = normalizedProps.getOptionalLong(STATISTICS_ROW_COUNT);
		if (rowCount.isPresent()) {
			Map<String, ColumnStats> columnStats = readColumnStats(normalizedProps, STATISTICS_COLUMNS);
			return Optional.of(new TableStats(rowCount.get(), columnStats));
		} else {
			return Optional.empty();
		}
	}

	// ----------------------------------------------------------------------------------------------
	// Getters
	// ----------------------------------------------------------------------------------------------

	/**
	 * Returns whether this external table is declared as table source.
	 */
	public boolean isTableSource() {
		return isSource;
	}

	/**
	 * Returns whether this external table is declared as table sink.
	 */
	public boolean isTableSink() {
		return isSink;
	}

	/**
	 * Returns whether this external table is intended for batch environments.
	 */
	public boolean isBatchTable() {
		return isBatch;
	}

	/**
	 * Returns whether this external table is intended for stream environments.
	 */
	public boolean isStreamTable() {
		return isStreaming;
	}

	// ----------------------------------------------------------------------------------------------

	/**
	 * Converts this descriptor into a set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		return properties;
	}
}
