/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Mode that defines how data is exchanged between tasks if the shuffling behavior has not been set
 * explicitly for an individual exchange.
 *
 * <p>The shuffle mode depends on the configured {@link ExecutionOptions#RUNTIME_MODE} and is only
 * relevant for batch executions on bounded streams.
 *
 * <p>In streaming mode, upstream and downstream tasks run simultaneously to achieve low latency. An
 * exchange is always pipelined (i.e. a result record is immediately sent to and processed by the
 * downstream task). Thus, the receiver back-pressures the sender.
 *
 * <p>In batch mode, upstream and downstream tasks can run in stages. Blocking exchanges persist
 * records to some storage. Downstream tasks then fetch these records after the upstream tasks
 * finished. Such an exchange reduces the resources required to execute the job as it does not need
 * to run upstream and downstream tasks simultaneously.
 */
@PublicEvolving
public enum ShuffleMode implements DescribedEnum {

    /**
     * Upstream and downstream tasks run simultaneously.
     *
     * <p>This leads to lower latency and more evenly distributed (but higher) resource usage across
     * tasks in batch mode.
     *
     * <p>This is the only supported shuffle behavior in streaming mode.
     */
    ALL_EXCHANGES_PIPELINED(
            text(
                    "Upstream and downstream tasks run simultaneously. This leads to lower latency "
                            + "and more evenly distributed (but higher) resource usage across tasks "
                            + "in batch mode. This is the only supported shuffle behavior in streaming "
                            + "mode.")),

    /**
     * Upstream and downstream tasks run subsequently.
     *
     * <p>This reduces the resource usage in batch mode as downstream tasks are started after
     * upstream tasks finished.
     *
     * <p>This shuffle behavior is not supported in streaming mode.
     */
    ALL_EXCHANGES_BLOCKING(
            text(
                    "Upstream and downstream tasks run subsequently. This reduces the resource usage "
                            + "in batch mode as downstream tasks are started after upstream tasks "
                            + "finished. This shuffle behavior is not supported in streaming mode.")),

    /**
     * The framework chooses an appropriate shuffle behavior based on the {@link
     * ExecutionOptions#RUNTIME_MODE} and slot assignment.
     */
    AUTOMATIC(
            text(
                    "The framework chooses an appropriate shuffle behavior based on the runtime mode "
                            + "and slot assignment."));

    private final InlineElement description;

    ShuffleMode(InlineElement description) {
        this.description = description;
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }
}
