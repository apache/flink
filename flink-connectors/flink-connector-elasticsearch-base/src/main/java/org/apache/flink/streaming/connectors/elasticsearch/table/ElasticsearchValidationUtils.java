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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Utility methods for validating Elasticsearch properties.
 */
@Internal
class ElasticsearchValidationUtils {

	private static final Set<LogicalTypeRoot> ILLEGAL_PRIMARY_KEY_TYPES = new LinkedHashSet<>();

	static {
		ILLEGAL_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.ARRAY);
		ILLEGAL_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.MAP);
		ILLEGAL_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.MULTISET);
		ILLEGAL_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.STRUCTURED_TYPE);
		ILLEGAL_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.ROW);
		ILLEGAL_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.RAW);
		ILLEGAL_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.BINARY);
		ILLEGAL_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.VARBINARY);
	}

	/**
	 * Checks that the table does not have primary key defined on illegal types.
	 * In Elasticsearch the primary key is used to calculate the Elasticsearch document id,
	 * which is a string of up to 512 bytes. It cannot have whitespaces. As of now it is calculated
	 * by concatenating the fields. Certain types do not have a good string representation to be used
	 * in this scenario. The illegal types are mostly {@link LogicalTypeFamily#COLLECTION} types and
	 * {@link LogicalTypeRoot#RAW} type.
	 */
	public static void validatePrimaryKey(TableSchema schema) {
		schema.getPrimaryKey().ifPresent(
			key -> {
				List<LogicalTypeRoot> illegalTypes = key.getColumns()
					.stream()
					.map(fieldName -> {
						LogicalType logicalType = schema.getFieldDataType(fieldName).get().getLogicalType();
						if (hasRoot(logicalType, LogicalTypeRoot.DISTINCT_TYPE)) {
							return ((DistinctType) logicalType).getSourceType().getTypeRoot();
						} else {
							return logicalType.getTypeRoot();
						}
					})
					.filter(ILLEGAL_PRIMARY_KEY_TYPES::contains)
					.collect(Collectors.toList());

				if (!illegalTypes.isEmpty()) {
					throw new ValidationException(
						String.format(
							"The table has a primary key on columns of illegal types: %s.\n" +
								" Elasticsearch sink does not support primary keys on columns of types: %s.",
							illegalTypes,
							ILLEGAL_PRIMARY_KEY_TYPES));
				}
			}
		);
	}

	private ElasticsearchValidationUtils() {
	}
}
