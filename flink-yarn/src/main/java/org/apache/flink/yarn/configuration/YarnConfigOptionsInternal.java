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

package org.apache.flink.yarn.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerImpl;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Yarn configuration options that are not meant to be set by the user. */
@Internal
public class YarnConfigOptionsInternal {

    public static final ConfigOption<String> APPLICATION_LOG_CONFIG_FILE =
            key("$internal.yarn.log-config-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "**DO NOT USE** The location of the log config file, e.g. the path to your log4j.properties for log4j.");

    /**
     * **DO NOT USE** Whether {@link org.apache.flink.yarn.YarnResourceManagerDriver} should match
     * the vcores of allocated containers with those requested.
     *
     * <p>By default, Yarn ignores vcores in the container requests, and always allocate 1 vcore for
     * each container. Iff 'yarn.scheduler.capacity.resource-calculator' is set to
     * 'DominantResourceCalculator' for Yarn, will it allocate container vcores as requested.
     *
     * <p>For Hadoop 2.6+, we can learn whether Yarn matches vcores from {@link
     * org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse}. However, this
     * is not available to earlier Hadoop versions (pre 2.6). Therefore, for earlier Hadoop
     * versions, the user needs to make sure this configuration option is consistent with the Yarn
     * setup.
     *
     * <p>ATM, it should be fine to keep this option 'false', because with the current {@link
     * SlotManagerImpl} all the TM containers should have the same resources. If later we add
     * another {@link SlotManager} implementation that may have TMs with different resources, and if
     * we need it to work with pre 2.6 Hadoop versions, we can expose this configuration option to
     * users.
     */
    public static final ConfigOption<Boolean> MATCH_CONTAINER_VCORES =
            key("$internal.yarn.resourcemanager.enable-vcore-matching")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "**DO NOT USE** Whether YarnResourceManagerDriver should match the container vcores.");
}
