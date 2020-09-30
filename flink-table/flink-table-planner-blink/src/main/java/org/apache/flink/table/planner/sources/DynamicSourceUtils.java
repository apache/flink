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

package org.apache.flink.table.planner.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.abilities.SupportsComputedColumnPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.List;

/**
 * Utilities for dealing with {@link DynamicTableSource}.
 */
@Internal
public final class DynamicSourceUtils {

	private static final List<Class<?>> UNSUPPORTED_ABILITIES = Arrays.asList(
		SupportsComputedColumnPushDown.class,
		SupportsWatermarkPushDown.class);

	/**
	 * Prepares the given {@link DynamicTableSource}. It check whether the source is compatible with the
	 * given schema.
	 */
	public static void prepareDynamicSource(
			ObjectIdentifier sourceIdentifier,
			CatalogTable table,
			DynamicTableSource source,
			boolean isStreamingMode) {

		validateAbilities(source);

		if (source instanceof ScanTableSource) {
			validateScanSource(sourceIdentifier, table, (ScanTableSource) source, isStreamingMode);
		}

		// lookup table source is validated in LookupJoin node
	}

	private static void validateScanSource(
			ObjectIdentifier sourceIdentifier,
			CatalogTable table,
			ScanTableSource scanSource,
			boolean isStreamingMode) {
		final ScanRuntimeProvider provider = scanSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		final ChangelogMode changelogMode = scanSource.getChangelogMode();

		validateWatermarks(sourceIdentifier, table.getSchema());

		if (isStreamingMode) {
			validateScanSourceForBatch(sourceIdentifier, changelogMode, provider);
		} else {
			validateScanSourceForStreaming(sourceIdentifier, scanSource, changelogMode);
		}
	}

	private static void validateScanSourceForStreaming(
			ObjectIdentifier sourceIdentifier,
			ScanTableSource scanSource,
			ChangelogMode changelogMode) {
		// sanity check for produced ChangelogMode
		final boolean hasUpdateBefore = changelogMode.contains(RowKind.UPDATE_BEFORE);
		final boolean hasUpdateAfter = changelogMode.contains(RowKind.UPDATE_AFTER);
		if (!hasUpdateBefore && hasUpdateAfter) {
			// only UPDATE_AFTER
			throw new TableException(
				String.format(
					"Unsupported source for table '%s'. Currently, a %s doesn't support a changelog which contains " +
						"UPDATE_AFTER but no UPDATE_BEFORE. Please adapt the implementation of class '%s'.",
					sourceIdentifier.asSummaryString(),
					ScanTableSource.class.getSimpleName(),
					scanSource.getClass().getName()
				)
			);
		} else if (hasUpdateBefore && !hasUpdateAfter) {
			// only UPDATE_BEFORE
			throw new ValidationException(
				String.format(
					"Invalid source for table '%s'. A %s doesn't support a changelog which contains " +
						"UPDATE_BEFORE but no UPDATE_AFTER. Please adapt the implementation of class '%s'.",
					sourceIdentifier.asSummaryString(),
					ScanTableSource.class.getSimpleName(),
					scanSource.getClass().getName()
				)
			);
		}
	}

	private static void validateScanSourceForBatch(
			ObjectIdentifier sourceIdentifier,
			ChangelogMode changelogMode,
			ScanRuntimeProvider provider) {
		// batch only supports bounded source
		if (!provider.isBounded()) {
			throw new ValidationException(
				String.format(
					"Querying an unbounded table '%s' in batch mode is not allowed. " +
						"The table source is unbounded.",
					sourceIdentifier.asSummaryString()
				)
			);
		}
		// batch only supports INSERT only source
		if (!changelogMode.containsOnly(RowKind.INSERT)) {
			throw new TableException(
				String.format(
					"Querying a table in batch mode is currently only possible for INSERT-only table sources. " +
						"But the source for table '%s' produces other changelog messages than just INSERT.",
					sourceIdentifier.asSummaryString()
				)
			);
		}
	}

	private static void validateWatermarks(
			ObjectIdentifier sourceIdentifier,
			TableSchema schema) {
		if (schema.getWatermarkSpecs().isEmpty()) {
			return;
		}

		if (schema.getWatermarkSpecs().size() > 1) {
			throw new TableException(
				String.format(
					"Currently only at most one WATERMARK declaration is supported for table '%s'.",
					sourceIdentifier.asSummaryString()
				)
			);
		}

		final String rowtimeAttribute = schema.getWatermarkSpecs().get(0).getRowtimeAttribute();
		if (rowtimeAttribute.contains(".")) {
			throw new TableException(
				String.format(
					"A nested field '%s' cannot be declared as rowtime attribute for table '%s' right now.",
					rowtimeAttribute,
					sourceIdentifier.asSummaryString()
				)
			);
		}
	}

	private static void validateAbilities(DynamicTableSource source) {
		UNSUPPORTED_ABILITIES.forEach(ability -> {
			if (ability.isAssignableFrom(source.getClass())) {
				throw new UnsupportedOperationException(
					String.format(
						"Currently, a %s with %s ability is not supported.",
						DynamicTableSource.class,
						ability.getSimpleName())
				);
			}
		});
	}

	private DynamicSourceUtils() {
		// no instantiation
	}
}
