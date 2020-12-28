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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.description.Description;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * This class holds configuration constants used by Flink's YARN runners.
 *
 * <p>These options are not expected to be ever configured by users explicitly.
 */
public class YarnConfigOptions {

    /** The vcores used by YARN application master. */
    public static final ConfigOption<Integer> APP_MASTER_VCORES =
            key("yarn.appmaster.vcores")
                    .defaultValue(1)
                    .withDescription(
                            "The number of virtual cores (vcores) used by YARN application master.");

    /**
     * Defines whether user-jars are included in the system class path for per-job-clusters as well
     * as their positioning in the path. They can be positioned at the beginning ("FIRST"), at the
     * end ("LAST"), or be positioned based on their name ("ORDER"). "DISABLED" means the user-jars
     * are excluded from the system class path.
     */
    public static final ConfigOption<String> CLASSPATH_INCLUDE_USER_JAR =
            key("yarn.per-job-cluster.include-user-jar")
                    .defaultValue("ORDER")
                    .withDescription(
                            "Defines whether user-jars are included in the system class path for per-job-clusters as"
                                    + " well as their positioning in the path. They can be positioned at the beginning (\"FIRST\"), at the"
                                    + " end (\"LAST\"), or be positioned based on their name (\"ORDER\"). \"DISABLED\" means the user-jars"
                                    + " are excluded from the system class path.");

    /** The vcores exposed by YARN. */
    public static final ConfigOption<Integer> VCORES =
            key("yarn.containers.vcores")
                    .defaultValue(-1)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of virtual cores (vcores) per YARN container. By default, the number of vcores"
                                                    + " is set to the number of slots per TaskManager, if set, or to 1, otherwise. In order for this"
                                                    + " parameter to be used your cluster must have CPU scheduling enabled. You can do this by setting"
                                                    + " the %s.",
                                            code(
                                                    "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"))
                                    .build());

    /**
     * Set the number of retries for failed YARN ApplicationMasters/JobManagers in high availability
     * mode. This value is usually limited by YARN. By default, it's 1 in the standalone case and 2
     * in the high availability case.
     *
     * <p>>Note: This option returns a String since Integer options must have a static default
     * value.
     */
    public static final ConfigOption<String> APPLICATION_ATTEMPTS =
            key("yarn.application-attempts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Number of ApplicationMaster restarts. By default, the value will be set to 1. "
                                                    + "If high availability is enabled, then the default value will be 2. "
                                                    + "The restart number is also limited by YARN (configured via %s). "
                                                    + "Note that that the entire Flink cluster will restart and the YARN Client will lose the connection.",
                                            link(
                                                    "https://hadoop.apache.org/docs/r2.4.1/hadoop-yarn/hadoop-yarn-common/yarn-default.xml",
                                                    "yarn.resourcemanager.am.max-attempts"))
                                    .build());

    /** The config parameter defining the attemptFailuresValidityInterval of Yarn application. */
    public static final ConfigOption<Long> APPLICATION_ATTEMPT_FAILURE_VALIDITY_INTERVAL =
            key("yarn.application-attempt-failures-validity-interval")
                    .defaultValue(10000L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Time window in milliseconds which defines the number of application attempt failures when restarting the AM. "
                                                    + "Failures which fall outside of this window are not being considered. "
                                                    + "Set this value to -1 in order to count globally. "
                                                    + "See %s for more information.",
                                            link(
                                                    "https://hortonworks.com/blog/apache-hadoop-yarn-hdp-2-2-fault-tolerance-features-long-running-services/",
                                                    "here"))
                                    .build());

    /** The heartbeat interval between the Application Master and the YARN Resource Manager. */
    public static final ConfigOption<Integer> HEARTBEAT_DELAY_SECONDS =
            key("yarn.heartbeat.interval")
                    .defaultValue(5)
                    .withDeprecatedKeys("yarn.heartbeat-delay")
                    .withDescription(
                            "Time between heartbeats with the ResourceManager in seconds.");

    /**
     * The heartbeat interval between the Application Master and the YARN Resource Manager if Flink
     * is requesting containers.
     */
    public static final ConfigOption<Integer> CONTAINER_REQUEST_HEARTBEAT_INTERVAL_MILLISECONDS =
            key("yarn.heartbeat.container-request-interval")
                    .defaultValue(500)
                    .withDescription(
                            new Description.DescriptionBuilder()
                                    .text(
                                            "Time between heartbeats with the ResourceManager in milliseconds if Flink requests containers:")
                                    .list(
                                            text(
                                                    "The lower this value is, the faster Flink will get notified about container allocations since requests and allocations are transmitted via heartbeats."),
                                            text(
                                                    "The lower this value is, the more excessive containers might get allocated which will eventually be released but put pressure on Yarn."))
                                    .text(
                                            "If you observe too many container allocations on the ResourceManager, then it is recommended to increase this value. See %s for more information.",
                                            link(
                                                    "https://issues.apache.org/jira/browse/YARN-1902",
                                                    "this link"))
                                    .build());

    /**
     * When a Flink job is submitted to YARN, the JobManager's host and the number of available
     * processing slots is written into a properties file, so that the Flink client is able to pick
     * those details up. This configuration parameter allows changing the default location of that
     * file (for example for environments sharing a Flink installation between users)
     */
    public static final ConfigOption<String> PROPERTIES_FILE_LOCATION =
            key("yarn.properties-file.location")
                    .noDefaultValue()
                    .withDescription(
                            "When a Flink job is submitted to YARN, the JobManager’s host and the number of available"
                                    + " processing slots is written into a properties file, so that the Flink client is able to pick those"
                                    + " details up. This configuration parameter allows changing the default location of that file"
                                    + " (for example for environments sharing a Flink installation between users).");

    /**
     * The config parameter defining the Akka actor system port for the ApplicationMaster and
     * JobManager. The port can either be a port, such as "9123", a range of ports: "50100-50200" or
     * a list of ranges and or points: "50100-50200,50300-50400,51234". Setting the port to 0 will
     * let the OS choose an available port.
     */
    public static final ConfigOption<String> APPLICATION_MASTER_PORT =
            key("yarn.application-master.port")
                    .defaultValue("0")
                    .withDescription(
                            "With this configuration option, users can specify a port, a range of ports or a list of ports"
                                    + " for the Application Master (and JobManager) RPC port. By default we recommend using the default value (0)"
                                    + " to let the operating system choose an appropriate port. In particular when multiple AMs are running on"
                                    + " the same physical host, fixed port assignments prevent the AM from starting. For example when running"
                                    + " Flink on YARN on an environment with a restrictive firewall, this option allows specifying a range of"
                                    + " allowed ports.");

    /**
     * A non-negative integer indicating the priority for submitting a Flink YARN application. It
     * will only take effect if YARN priority scheduling setting is enabled. Larger integer
     * corresponds with higher priority. If priority is negative or set to '-1'(default), Flink will
     * unset yarn priority setting and use cluster default priority.
     *
     * @see <a
     *     href="https://hadoop.apache.org/docs/r2.8.5/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html">YARN
     *     Capacity Scheduling Doc</a>
     */
    public static final ConfigOption<Integer> APPLICATION_PRIORITY =
            key("yarn.application.priority")
                    .defaultValue(-1)
                    .withDescription(
                            "A non-negative integer indicating the priority for submitting a Flink YARN application. It"
                                    + " will only take effect if YARN priority scheduling setting is enabled. Larger integer corresponds"
                                    + " with higher priority. If priority is negative or set to '-1'(default), Flink will unset yarn priority"
                                    + " setting and use cluster default priority. Please refer to YARN's official documentation for specific"
                                    + " settings required to enable priority scheduling for the targeted YARN version.");

    /**
     * Yarn session client uploads flink jar and user libs to file system (hdfs/s3) as local
     * resource for yarn application context. The replication number changes the how many replica of
     * each of these files in hdfs/s3. It is useful to accelerate this container bootstrap, when a
     * Flink application needs more than one hundred of containers. If it is not configured, Flink
     * will use the default replication value in hadoop configuration.
     */
    public static final ConfigOption<Integer> FILE_REPLICATION =
            key("yarn.file-replication")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Number of file replication of each local resource file. If it is not configured, Flink will"
                                    + " use the default replication value in hadoop configuration.");

    /** A comma-separated list of strings to use as YARN application tags. */
    public static final ConfigOption<String> APPLICATION_TAGS =
            key("yarn.tags")
                    .defaultValue("")
                    .withDescription(
                            "A comma-separated list of tags to apply to the Flink YARN application.");

    // ----------------------- YARN CLI OPTIONS ------------------------------------

    public static final ConfigOption<String> STAGING_DIRECTORY =
            key("yarn.staging-directory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Staging directory used to store YARN files while submitting applications. Per default, it uses the home directory of the configured file system.");

    public static final ConfigOption<List<String>> SHIP_FILES =
            key("yarn.ship-files")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDeprecatedKeys("yarn.ship-directories")
                    .withDescription(
                            "A semicolon-separated list of files and/or directories to be shipped to the YARN cluster.");

    public static final ConfigOption<List<String>> SHIP_ARCHIVES =
            key("yarn.ship-archives")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated list of archives to be shipped to the YARN cluster."
                                    + " These archives will be un-packed when localizing and they can be any of the following types: "
                                    + "\".tar.gz\", \".tar\", \".tgz\", \".dst\", \".jar\", \".zip\".");

    public static final ConfigOption<String> FLINK_DIST_JAR =
            key("yarn.flink-dist-jar")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The location of the Flink dist jar.");

    public static final ConfigOption<String> APPLICATION_ID =
            key("yarn.application.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The YARN application id of the running yarn cluster."
                                    + " This is the YARN cluster where the pipeline is going to be executed.");

    public static final ConfigOption<String> APPLICATION_QUEUE =
            key("yarn.application.queue")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The YARN queue on which to put the current pipeline.");

    public static final ConfigOption<String> APPLICATION_NAME =
            key("yarn.application.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("A custom name for your YARN application.");

    public static final ConfigOption<String> APPLICATION_TYPE =
            key("yarn.application.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("A custom type for your YARN application..");

    public static final ConfigOption<String> NODE_LABEL =
            key("yarn.application.node-label")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specify YARN node label for the YARN application.");

    public static final ConfigOption<Boolean> SHIP_LOCAL_KEYTAB =
            key("yarn.security.kerberos.ship-local-keytab")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When this is true Flink will ship the keytab file configured via "
                                    + SecurityOptions.KERBEROS_LOGIN_KEYTAB.key()
                                    + " as a localized YARN resource.");

    public static final ConfigOption<String> LOCALIZED_KEYTAB_PATH =
            key("yarn.security.kerberos.localized-keytab-path")
                    .stringType()
                    .defaultValue("krb5.keytab")
                    .withDescription(
                            "Local (on NodeManager) path where kerberos keytab file will be"
                                    + " localized to. If "
                                    + SHIP_LOCAL_KEYTAB.key()
                                    + " set to "
                                    + "true, Flink willl ship the keytab file as a YARN local "
                                    + "resource. In this case, the path is relative to the local "
                                    + "resource directory. If set to false, Flink"
                                    + " will try to directly locate the keytab from the path itself.");

    public static final ConfigOption<List<String>> PROVIDED_LIB_DIRS =
            key("yarn.provided.lib.dirs")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated list of provided lib directories. They should be pre-uploaded and "
                                    + "world-readable. Flink will use them to exclude the local Flink jars(e.g. flink-dist, lib/, plugins/)"
                                    + "uploading to accelerate the job submission process. Also YARN will cache them on the nodes so that "
                                    + "they doesn't need to be downloaded every time for each application. An example could be "
                                    + "hdfs://$namenode_address/path/of/flink/lib");

    public static final ConfigOption<List<String>> YARN_ACCESS =
            key("yarn.security.kerberos.additionalFileSystems")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A comma-separated list of additional Kerberos-secured Hadoop filesystems Flink is going to access. For example, yarn.security.kerberos.additionalFileSystems=hdfs://namenode2:9002,hdfs://namenode3:9003. The client submitting to YARN needs to have access to these file systems to retrieve the security tokens.");

    /**
     * Defines the configuration key of that external resource in Yarn. This is used as a suffix in
     * an actual config.
     */
    public static final String EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX = "yarn.config-key";

    /**
     * If configured, Flink will add this key to the resource profile of container request to Yarn.
     * The value will be set to {@link ExternalResourceOptions#EXTERNAL_RESOURCE_AMOUNT}.
     *
     * <p>It is intentionally included into user docs while unused.
     */
    @SuppressWarnings("unused")
    public static final ConfigOption<String> EXTERNAL_RESOURCE_YARN_CONFIG_KEY =
            key(ExternalResourceOptions.genericKeyWithSuffix(
                            EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "If configured, Flink will add this key to the resource profile of container request to Yarn. "
                                    + "The value will be set to the value of "
                                    + ExternalResourceOptions.EXTERNAL_RESOURCE_AMOUNT.key()
                                    + ".");

    // ------------------------------------------------------------------------

    /** This class is not meant to be instantiated. */
    private YarnConfigOptions() {}

    /** @see YarnConfigOptions#CLASSPATH_INCLUDE_USER_JAR */
    public enum UserJarInclusion {
        DISABLED,
        FIRST,
        LAST,
        ORDER
    }
}
