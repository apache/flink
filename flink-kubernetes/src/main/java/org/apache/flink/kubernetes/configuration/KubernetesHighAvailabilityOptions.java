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

package org.apache.flink.kubernetes.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/** The set of configuration options relating to Kubernetes high-availability settings. */
@PublicEvolving
public class KubernetesHighAvailabilityOptions {

    @Documentation.Section(Documentation.Sections.EXPERT_KUBERNETES_HIGH_AVAILABILITY)
    public static final ConfigOption<Duration> KUBERNETES_LEASE_DURATION =
            key("high-availability.kubernetes.leader-election.lease-duration")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(15))
                    .withDescription(
                            "Define the lease duration for the Kubernetes leader election. The leader will "
                                    + "continuously renew its lease time to indicate its existence. And the followers will do a lease "
                                    + "checking against the current time. \"renewTime + leaseDuration > now\" means the leader is alive.");

    @Documentation.Section(Documentation.Sections.EXPERT_KUBERNETES_HIGH_AVAILABILITY)
    public static final ConfigOption<Duration> KUBERNETES_RENEW_DEADLINE =
            key("high-availability.kubernetes.leader-election.renew-deadline")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(15))
                    .withDescription(
                            "Defines the deadline duration when the leader tries to renew the lease. The leader will "
                                    + "give up its leadership if it cannot successfully renew the lease in the given time.");

    @Documentation.Section(Documentation.Sections.EXPERT_KUBERNETES_HIGH_AVAILABILITY)
    public static final ConfigOption<Duration> KUBERNETES_RETRY_PERIOD =
            key("high-availability.kubernetes.leader-election.retry-period")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription(
                            "Defines the pause duration between consecutive retries. All the contenders, including "
                                    + "the current leader and all other followers, periodically try to acquire/renew the leadership if "
                                    + "possible at this interval.");

    /** Not intended to be instantiated. */
    private KubernetesHighAvailabilityOptions() {}
}
