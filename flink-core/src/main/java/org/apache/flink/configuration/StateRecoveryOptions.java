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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.core.execution.RestoreMode;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The {@link ConfigOption configuration options} used when restoring state from a savepoint or a
 * checkpoint.
 */
@PublicEvolving
public class StateRecoveryOptions {

    /** The path to a savepoint that will be used to bootstrap the pipeline's state. */
    public static final ConfigOption<String> SAVEPOINT_PATH =
            key("execution.state-recovery.path")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("execution.savepoint.path")
                    .withDescription(
                            "Path to a savepoint to restore the job from (for example hdfs:///flink/savepoint-1537).");

    /**
     * A flag indicating if we allow Flink to skip savepoint state that cannot be restored, e.g.
     * because the corresponding operator has been removed.
     */
    public static final ConfigOption<Boolean> SAVEPOINT_IGNORE_UNCLAIMED_STATE =
            key("execution.state-recovery.ignore-unclaimed-state")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("execution.savepoint.ignore-unclaimed-state")
                    .withDescription(
                            "Allow to skip savepoint state that cannot be restored. "
                                    + "Allow this if you removed an operator from your pipeline after the savepoint was triggered.");
    /**
     * Describes the mode how Flink should restore from the given savepoint or retained checkpoint.
     */
    public static final ConfigOption<RestoreMode> RESTORE_MODE =
            key("execution.state-recovery.claim-mode")
                    .enumType(RestoreMode.class)
                    .defaultValue(RestoreMode.DEFAULT)
                    .withDeprecatedKeys("execution.savepoint-restore-mode")
                    .withDescription(
                            "Describes the mode how Flink should restore from the given"
                                    + " savepoint or retained checkpoint.");

    /**
     * Access to this option is officially only supported via {@link
     * org.apache.flink.runtime.jobgraph.CheckpointConfig#enableApproximateLocalRecovery(boolean)},
     * but there is no good reason behind this.
     */
    @Internal @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Boolean> APPROXIMATE_LOCAL_RECOVERY =
            key("execution.state-recovery.approximate-local-recovery")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("execution.checkpointing.approximate-local-recovery")
                    .withDescription("Flag to enable approximate local recovery.");

    public static final ConfigOption<Long> CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA =
            ConfigOptions.key("execution.state-recovery.without-channel-state.checkpoint-id")
                    .longType()
                    .defaultValue(-1L)
                    .withDeprecatedKeys(
                            "execution.checkpointing.recover-without-channel-state.checkpoint-id")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Checkpoint id for which in-flight data should be ignored in case of the recovery from this checkpoint.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "It is better to keep this value empty until "
                                                    + "there is explicit needs to restore from "
                                                    + "the specific checkpoint without in-flight data.")
                                    .linebreak()
                                    .build());

    /**
     * This option configures local recovery for the state backend, which indicates whether to
     * recovery from local snapshot. By default, local recovery is deactivated.
     *
     * <p>Local recovery currently only covers keyed state backends (including both the
     * EmbeddedRocksDBStateBackend and the HashMapStateBackend).
     */
    public static final ConfigOption<Boolean> LOCAL_RECOVERY =
            ConfigOptions.key("execution.state-recovery.from-local")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("state.backend.local-recovery")
                    .withDescription(
                            "This option configures local recovery for the state backend, "
                                    + "which indicates whether to recovery from local snapshot."
                                    + "By default, local recovery is deactivated. Local recovery currently only "
                                    + "covers keyed state backends (including both the EmbeddedRocksDBStateBackend and the HashMapStateBackend).\"");
}
