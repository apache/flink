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

package org.apache.flink.table.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;

import java.util.Optional;

/**
 * Context provided by some {@link ScanRuntimeProvider} and {@link SinkRuntimeProvider} with
 * utilities for generating the runtime implementation.
 */
@PublicEvolving
public interface ProviderContext {

    /**
     * Generates a new unique identifier for a {@link Transformation}/{@code DataStream} operator.
     *
     * <p>UIDs are crucial for state management as they identify an operator in the topology. The
     * planner guarantees that all operators receive a stable UID. However, the planner does not
     * control the operators and transformations in sources and sinks. From a planner’s perspective,
     * sources and sinks are black boxes. Implementers must ensure that UIDs are assigned to all
     * operators.
     *
     * <p>The {@param name} argument must be unique within the provider implementation. The
     * framework will make sure that the name is unique for the entire topology.
     *
     * <p>For example, a connector that consists of two transformations (a source and a validator)
     * must use this method in the following way:
     *
     * <pre>{@code
     * var sourceOperator = ...
     * providerContext.generateUid("source").ifPresent(sourceOperator::uid);
     *
     * var validatorOperator = sourceOperator.process(...);
     * providerContext.generateUid("validator").ifPresent(validatorOperator::uid);
     * }</pre>
     *
     * <p>This method returns empty if an identifier cannot be generated, i.e., because the job is
     * in batch mode, or UIDs cannot be guaranteed to be unique because the topology is not created
     * from a compiled plan.
     */
    Optional<String> generateUid(String name);

    /**
     * Returns the framework's node type in which this connector is embedded.
     *
     * <p>In other words: It returns the ExecNode's name and version as contained in the compiled
     * plan. For example, "stream-exec-table-source-scan_2" or "stream-exec-sink_1".
     */
    default String getContainerNodeType() {
        return "";
    }

    /**
     * Returns the display name provided by the framework to label the connector.
     *
     * <p>If multiple transformations are present, the implementer can decide which one is the
     * connector. The name will be shown in UI and logs.
     *
     * <p>For example, a connector that consists of two transformations (a source and a validator)
     * can use this method in the following way:
     *
     * <pre>{@code
     * var sourceOperator = ...
     * sourceOperator.name(providerContext.getName());
     *
     * var validatorOperator = sourceOperator.process(...);
     * validatorOperator.name("Validator for " + providerContext.getName());
     * }</pre>
     */
    default String getName() {
        return "";
    }

    /**
     * Returns the description provided by the framework to label the connector.
     *
     * <p>If multiple transformations are present, the implementer can decide which one is the
     * connector. The description will be shown in UI and logs.
     *
     * <p>For example, a connector that consists of two transformations (a source and a validator)
     * can use this method in the following way:
     *
     * <pre>{@code
     * var sourceOperator = ...
     * sourceOperator.setDescription(providerContext.getDescription());
     *
     * var validatorOperator = sourceOperator.process(...);
     * validatorOperator.setDescription("Validates non-null values.");
     * }</pre>
     */
    default String getDescription() {
        return "";
    }
}
