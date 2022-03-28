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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.RowKind;

import java.util.Map;

import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getValueDecodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;

/** Util class for source and sink validation rules. */
public class PulsarTableValidationUtils {
    public static void validatePrimaryKeyConstraints(
            ObjectIdentifier tableName,
            int[] primaryKeyIndexes,
            Map<String, String> options,
            FactoryUtil.TableFactoryHelper helper) {
        final DecodingFormat<DeserializationSchema<RowData>> format =
                getValueDecodingFormat(helper);
        if (primaryKeyIndexes.length > 0
                && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration configuration = Configuration.fromMap(options);
            String formatName = configuration.getOptional(FactoryUtil.FORMAT).get();
            // TODO change description
            throw new ValidationException(
                    String.format(
                            "The Pulsar table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                                    + " on the table, because it can't guarantee the semantic of primary key.",
                            tableName.asSummaryString(), formatName));
        }
    }

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateStartCursorConfigs(tableOptions);
    }

    private static void validateStartCursorConfigs(ReadableConfig tableOptions) {
        if (tableOptions.getOptional(SOURCE_START_FROM_MESSAGE_ID).isPresent()
                && tableOptions.getOptional(SOURCE_START_FROM_PUBLISH_TIME).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Only one of %s and %s can be specified. Detected both of them",
                            SOURCE_START_FROM_MESSAGE_ID.toString(),
                            SOURCE_START_FROM_PUBLISH_TIME.toString()));
        }
    }
}
