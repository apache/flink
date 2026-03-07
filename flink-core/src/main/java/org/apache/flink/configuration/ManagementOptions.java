/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Configuration options for cluster management features. */
@PublicEvolving
public class ManagementOptions {

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Boolean> NODE_QUARANTINE_ENABLED =
            key("cluster.management.node-quarantine.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "A flag to enable or disable the management node quarantine functionality. "
                                    + "When enabled, TaskManagers can be manually added to a quarantine list "
                                    + "via REST API to prevent new slots from being allocated on them. "
                                    + "This is independent of batch execution blocklist and speculative execution.");

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Duration> NODE_QUARANTINE_DEFAULT_DURATION =
            key("cluster.management.node-quarantine.default-duration")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The default duration for which a TaskManager should be quarantined "
                                    + "when added to the management node quarantine without specifying "
                                    + "an explicit duration.");

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Duration> NODE_QUARANTINE_MAX_DURATION =
            key("cluster.management.node-quarantine.max-duration")
                    .durationType()
                    .defaultValue(Duration.ofHours(24))
                    .withDescription(
                            "The maximum duration for which a TaskManager can be quarantined in the "
                                    + "management node quarantine. This prevents accidentally quarantining "
                                    + "TaskManagers for too long.");

    private ManagementOptions() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }
}
