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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Utility methods for validating Elasticsearch properties. */
@Internal
class ElasticsearchValidationUtils {
    private static final Set<LogicalTypeRoot> ALLOWED_PRIMARY_KEY_TYPES = new LinkedHashSet<>();

    static {
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.CHAR);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.VARCHAR);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.BOOLEAN);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.DECIMAL);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.TINYINT);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.SMALLINT);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.INTEGER);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.BIGINT);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.FLOAT);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.DOUBLE);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.DATE);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.INTERVAL_YEAR_MONTH);
        ALLOWED_PRIMARY_KEY_TYPES.add(LogicalTypeRoot.INTERVAL_DAY_TIME);
    }

    /**
     * Checks that the table does not have a primary key defined on illegal types. In Elasticsearch
     * the primary key is used to calculate the Elasticsearch document id, which is a string of up
     * to 512 bytes. It cannot have whitespaces. As of now it is calculated by concatenating the
     * fields. Certain types do not have a good string representation to be used in this scenario.
     * The illegal types are mostly {@link LogicalTypeFamily#COLLECTION} types and {@link
     * LogicalTypeRoot#RAW} type.
     */
    public static void validatePrimaryKey(DataType primaryKeyDataType) {
        List<DataType> fieldDataTypes = DataType.getFieldDataTypes(primaryKeyDataType);
        List<LogicalTypeRoot> illegalTypes =
                fieldDataTypes.stream()
                        .map(DataType::getLogicalType)
                        .map(
                                logicalType -> {
                                    if (logicalType.is(LogicalTypeRoot.DISTINCT_TYPE)) {
                                        return ((DistinctType) logicalType)
                                                .getSourceType()
                                                .getTypeRoot();
                                    } else {
                                        return logicalType.getTypeRoot();
                                    }
                                })
                        .filter(t -> !ALLOWED_PRIMARY_KEY_TYPES.contains(t))
                        .collect(Collectors.toList());
        if (!illegalTypes.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "The table has a primary key on columns of illegal types: %s.",
                            illegalTypes));
        }
    }

    private ElasticsearchValidationUtils() {}
}
