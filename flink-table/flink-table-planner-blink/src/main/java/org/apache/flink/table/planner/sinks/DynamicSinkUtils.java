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

package org.apache.flink.table.planner.sinks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;

import java.util.List;
import java.util.Map;

/**
 * Utilities for dealing with {@link DynamicTableSink}.
 */
@Internal
public final class DynamicSinkUtils {

	/**
	 * Prepares the given {@link DynamicTableSink}. It check whether the sink is compatible with the
	 * INSERT INTO clause and applies initial parameters.
	 */
	public static void prepareDynamicSink(
			CatalogSinkModifyOperation sinkOperation,
			ObjectIdentifier sinkIdentifier,
			DynamicTableSink sink,
			List<String> partitionKeys) {

		validatePartitioning(sinkOperation, sinkIdentifier, sink, partitionKeys);

		validateAndApplyOverwrite(sinkOperation, sinkIdentifier, sink);
	}

	private static void validatePartitioning(
			CatalogSinkModifyOperation sinkOperation,
			ObjectIdentifier sinkIdentifier,
			DynamicTableSink sink,
			List<String> partitionKeys) {
		if (!partitionKeys.isEmpty()) {
			if (!(sink instanceof SupportsPartitioning)) {
				throw new TableException(
					String.format(
						"Table '%s' is a partitioned table, but the underlying %s doesn't " +
							"implement the %s interface.",
						sinkIdentifier.asSummaryString(),
						DynamicTableSink.class.getSimpleName(),
						SupportsPartitioning.class.getSimpleName()
					)
				);
			}
		}

		final Map<String, String> staticPartitions = sinkOperation.getStaticPartitions();
		staticPartitions.keySet().forEach(p -> {
			if (!partitionKeys.contains(p)) {
				throw new ValidationException(
					String.format(
						"Static partition column '%s' should be in the partition keys list %s for table '%s'.",
						p,
						partitionKeys,
						sinkIdentifier.asSummaryString()
					)
				);
			}
		});
	}

	private static void validateAndApplyOverwrite(
			CatalogSinkModifyOperation sinkOperation,
			ObjectIdentifier sinkIdentifier,
			DynamicTableSink sink) {
		if (!sinkOperation.isOverwrite()) {
			return;
		}
		if (!(sink instanceof SupportsOverwrite)) {
			throw new ValidationException(
				String.format(
					"INSERT OVERWRITE requires that the underlying %s of table '%s' " +
						"implements the %s interface.",
					DynamicTableSink.class.getSimpleName(),
					sinkIdentifier.asSummaryString(),
					SupportsOverwrite.class.getSimpleName()
				)
			);
		}
		final SupportsOverwrite overwriteSink = (SupportsOverwrite) sink;
		overwriteSink.applyOverwrite(sinkOperation.isOverwrite());
	}

	private DynamicSinkUtils() {
		// no instantiation
	}
}
