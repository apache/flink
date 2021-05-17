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

package org.apache.flink.mesos.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to mesos settings.
 *
 * @deprecated Apache Mesos support was deprecated in Flink 1.13 and is subject to removal in the
 *     future (see FLINK-22352 for further details).
 */
@Deprecated
public class MesosOptions {

    /**
     * The Mesos master URL.
     *
     * <p>The value should be in one of the following forms:
     *
     * <pre>{@code
     * host:port
     * zk://host1:port1,host2:port2,.../path
     * zk://username:password@host1:port1,host2:port2,.../path
     * file:///path/to/file (where file contains one of the above)
     * }</pre>
     */
    public static final ConfigOption<String> MASTER_URL =
            key("mesos.master")
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The Mesos master URL. The value should be in one of the following forms: ")
                                    .list(
                                            TextElement.text("host:port"),
                                            TextElement.text(
                                                    "zk://host1:port1,host2:port2,.../path"),
                                            TextElement.text(
                                                    "zk://username:password@host1:port1,host2:port2,.../path"),
                                            TextElement.text("file:///path/to/file"))
                                    .build());

    /**
     * The failover timeout for the Mesos scheduler, after which running tasks are automatically
     * shut down.
     */
    public static final ConfigOption<Integer> FAILOVER_TIMEOUT_SECONDS =
            key("mesos.failover-timeout")
                    .defaultValue(60 * 60 * 24 * 7)
                    .withDescription(
                            "The failover timeout in seconds for the Mesos scheduler, after which running tasks are"
                                    + " automatically shut down.");

    /**
     * The config parameter defining the Mesos artifact server port to use. Setting the port to 0
     * will let the OS choose an available port.
     */
    public static final ConfigOption<Integer> ARTIFACT_SERVER_PORT =
            key("mesos.resourcemanager.artifactserver.port")
                    .defaultValue(0)
                    .withDescription(
                            "The config parameter defining the Mesos artifact server port to use. Setting the port to"
                                    + " 0 will let the OS choose an available port.");

    public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_NAME =
            key("mesos.resourcemanager.framework.name")
                    .defaultValue("Flink")
                    .withDescription("Mesos framework name");

    public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_ROLE =
            key("mesos.resourcemanager.framework.role")
                    .defaultValue("*")
                    .withDescription("Mesos framework role definition");

    public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_PRINCIPAL =
            key("mesos.resourcemanager.framework.principal")
                    .noDefaultValue()
                    .withDescription("Mesos framework principal");

    public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_SECRET =
            key("mesos.resourcemanager.framework.secret")
                    .noDefaultValue()
                    .withDescription("Mesos framework secret");

    public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_USER =
            key("mesos.resourcemanager.framework.user")
                    .defaultValue("")
                    .withDescription("Mesos framework user");

    /** Config parameter to override SSL support for the Artifact Server. */
    public static final ConfigOption<Boolean> ARTIFACT_SERVER_SSL_ENABLED =
            key("mesos.resourcemanager.artifactserver.ssl.enabled")
                    .defaultValue(true)
                    .withDescription(
                            "Enables SSL for the Flink artifact server. Note that security.ssl.enabled also needs to"
                                    + " be set to true encryption to enable encryption.");

    /**
     * Config parameter to configure which configuration keys will dynamically get a port assigned
     * through Mesos.
     */
    public static final ConfigOption<String> PORT_ASSIGNMENTS =
            key("mesos.resourcemanager.tasks.port-assignments")
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Comma-separated list of configuration keys which represent a configurable port. "
                                                    + "All port keys will dynamically get a port assigned through Mesos.")
                                    .build());

    /**
     * Config parameter to configure the amount of time to wait for unused expired Mesos offers
     * before they are declined.
     */
    public static final ConfigOption<Long> UNUSED_OFFER_EXPIRATION =
            key("mesos.resourcemanager.unused-offer-expiration")
                    .defaultValue(120000L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Amount of time to wait for unused expired offers before declining them. "
                                                    + "This ensures your scheduler will not hoard unuseful offers.")
                                    .build());

    /**
     * Config parameter to configure the amount of time refuse a particular offer for. This ensures
     * the same resource offer isn't resent immediately after declining.
     */
    public static final ConfigOption<Long> DECLINED_OFFER_REFUSE_DURATION =
            key("mesos.resourcemanager.declined-offer-refuse-duration")
                    .defaultValue(5000L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Amount of time to ask the Mesos master to not resend a "
                                                    + "declined resource offer again. This ensures a declined resource offer "
                                                    + "isn't resent immediately after being declined")
                                    .build());
}
