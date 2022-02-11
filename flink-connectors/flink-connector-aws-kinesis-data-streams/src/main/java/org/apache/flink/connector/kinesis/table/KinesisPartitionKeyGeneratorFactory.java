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

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import java.util.List;

import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER_FIELD_DELIMITER;

/** Factory Class for {@link PartitionKeyGenerator}. */
@Internal
public class KinesisPartitionKeyGeneratorFactory {

    // -----------------------------------------------------------------------------------------
    // Option enumerations
    // -----------------------------------------------------------------------------------------

    public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";
    public static final String SINK_PARTITIONER_VALUE_RANDOM = "random";

    /**
     * Constructs the kinesis partitioner for a {@code targetTable} based on the currently set
     * {@code tableOptions}.
     *
     * <p>The following rules are applied with decreasing precedence order.
     *
     * <ul>
     *   <li>If {@code targetTable} is partitioned, return a {@code RowDataKinesisPartitioner}.
     *   <li>If the partitioner type is not set, return a {@link
     *       RandomKinesisPartitionKeyGenerator}.
     *   <li>If a specific partitioner type alias is used, instantiate the corresponding type
     *   <li>Interpret the partitioner type as a classname of a user-defined partitioner.
     * </ul>
     *
     * @param tableOptions A read-only set of config options that determines the partitioner type.
     * @param physicalType Physical type for partitioning.
     * @param partitionKeys Partitioning keys in physical type.
     * @param classLoader A {@link ClassLoader} to use for loading user-defined partitioner classes.
     */
    public static PartitionKeyGenerator<RowData> getKinesisPartitioner(
            ReadableConfig tableOptions,
            RowType physicalType,
            List<String> partitionKeys,
            ClassLoader classLoader) {

        if (!partitionKeys.isEmpty()) {
            String delimiter = tableOptions.get(SINK_PARTITIONER_FIELD_DELIMITER);
            return new RowDataFieldsKinesisPartitionKeyGenerator(
                    physicalType, partitionKeys, delimiter);
        } else if (!tableOptions.getOptional(SINK_PARTITIONER).isPresent()) {
            return new RandomKinesisPartitionKeyGenerator<>();
        } else {
            String partitioner = tableOptions.getOptional(SINK_PARTITIONER).get();
            if (SINK_PARTITIONER_VALUE_FIXED.equals(partitioner)) {
                return new FixedKinesisPartitionKeyGenerator<>();
            } else if (SINK_PARTITIONER_VALUE_RANDOM.equals(partitioner)) {
                return new RandomKinesisPartitionKeyGenerator<>();
            } else { // interpret the option value as a fully-qualified class name
                return initializePartitioner(partitioner, classLoader);
            }
        }
    }

    /** Returns a class value with the given class name. */
    private static <T> PartitionKeyGenerator<T> initializePartitioner(
            String name, ClassLoader classLoader) {
        try {
            Class<?> clazz = Class.forName(name, true, classLoader);
            if (!PartitionKeyGenerator.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format(
                                "Partitioner class '%s' should have %s in its parents chain",
                                name, PartitionKeyGenerator.class.getName()));
            }
            @SuppressWarnings("unchecked")
            final PartitionKeyGenerator<T> partitioner =
                    InstantiationUtil.instantiate(name, PartitionKeyGenerator.class, classLoader);

            return partitioner;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format("Could not find and instantiate partitioner class '%s'", name),
                    e);
        }
    }
}
