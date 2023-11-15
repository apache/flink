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
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.yarn.configuration.YarnConfigOptions.UserJarInclusion.ORDER;

/**
 * This class holds configuration constants used by Flink's YARN runners.
 *
 * <p>These options are not expected to be ever configured by users explicitly.
 */
public class YarnConfigOptions {

    /** The vcores used by YARN application master. */
    public static final ConfigOption<Integer> APP_MASTER_VCORES =
            key("yarn.appmaster.vcores")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of virtual cores (vcores) used by YARN application master.");

    /**
     * Defines whether user-jars are included in the system class path as well as their positioning
     * in the path. They can be positioned at the beginning (FIRST), at the end (LAST), or be
     * positioned based on their name (ORDER). DISABLED means the user-jars are excluded from the
     * system class path and as a result these jars will be loaded by user classloader.
     */
    public static final ConfigOption<UserJarInclusion> CLASSPATH_INCLUDE_USER_JAR =
            key("yarn.classpath.include-user-jar")
                    .enumType(UserJarInclusion.class)
                    .defaultValue(ORDER)
                    .withDeprecatedKeys("yarn.per-job-cluster.include-user-jar")
                    .withDescription(
                            "Defines whether user-jars are included in the system class path "
                                    + "as well as their positioning in the path.");

    /** The vcores exposed by YARN. */
    public static final ConfigOption<Integer> VCORES =
            key("yarn.containers.vcores")
                    .intType()
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
                                                    + "Note that the entire Flink cluster will restart and the YARN Client will lose the connection.",
                                            link(
                                                    "https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-common/yarn-default.xml",
                                                    "yarn.resourcemanager.am.max-attempts"))
                                    .build());

    /** The config parameter defining the attemptFailuresValidityInterval of Yarn application. */
    public static final ConfigOption<Long> APPLICATION_ATTEMPT_FAILURE_VALIDITY_INTERVAL =
            key("yarn.application-attempt-failures-validity-interval")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Time window in milliseconds which defines the number of application attempt failures when restarting the AM. "
                                                    + "Failures which fall outside of this window are not being considered. "
                                                    + "Set this value to -1 in order to count globally. "
                                                    + "See %s for more information.",
                                            link(
                                                    "https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Application_Attempts_API",
                                                    "here"))
                                    .build());

    /** The heartbeat interval between the Application Master and the YARN Resource Manager. */
    public static final ConfigOption<Integer> HEARTBEAT_DELAY_SECONDS =
            key("yarn.heartbeat.interval")
                    .intType()
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
                    .intType()
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
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "When a Flink job is submitted to YARN, the JobManager’s host and the number of available"
                                    + " processing slots is written into a properties file, so that the Flink client is able to pick those"
                                    + " details up. This configuration parameter allows changing the default location of that file"
                                    + " (for example for environments sharing a Flink installation between users).");

    /**
     * The config parameter defining the Pekko actor system port for the ApplicationMaster and
     * JobManager. The port can either be a port, such as "9123", a range of ports: "50100-50200" or
     * a list of ranges and or points: "50100-50200,50300-50400,51234". Setting the port to 0 will
     * let the OS choose an available port.
     */
    public static final ConfigOption<String> APPLICATION_MASTER_PORT =
            key("yarn.application-master.port")
                    .stringType()
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
     *     href="https://hadoop.apache.org/docs/r2.10.2/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html">YARN
     *     Capacity Scheduling Doc</a>
     */
    public static final ConfigOption<Integer> APPLICATION_PRIORITY =
            key("yarn.application.priority")
                    .intType()
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
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "A comma-separated list of tags to apply to the Flink YARN application.");

    /**
     * Users and groups to give VIEW access.
     * https://www.cloudera.com/documentation/enterprise/latest/topics/cm_mc_yarn_acl.html
     */
    public static final ConfigOption<String> APPLICATION_VIEW_ACLS =
            key("yarn.view.acls")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Users and groups to give VIEW access. The ACLs are of for"
                                    + " comma-separated-users&lt;space&gt;comma-separated-groups."
                                    + " Wildcard ACL is also supported. The only valid wildcard ACL"
                                    + " is *, which grants permission to all users and groups.");

    /** Users and groups to give MODIFY access. */
    public static final ConfigOption<String> APPLICATION_MODIFY_ACLS =
            key("yarn.modify.acls")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Users and groups to give MODIFY access. The ACLs are of for"
                                    + " comma-separated-users&lt;space&gt;comma-separated-groups."
                                    + " Wildcard ACL is also supported. The only valid wildcard ACL"
                                    + " is *, which grants permission to all users and groups.");

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
                            "A semicolon-separated list of files and/or directories to be shipped to the YARN "
                                    + "cluster. These files/directories can come from the local path of flink client "
                                    + "or HDFS. For example, "
                                    + "\"/path/to/local/file;/path/to/local/directory;"
                                    + "hdfs://$namenode_address/path/of/file;"
                                    + "hdfs://$namenode_address/path/of/directory\"");

    public static final ConfigOption<List<String>> SHIP_ARCHIVES =
            key("yarn.ship-archives")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated list of archives to be shipped to the YARN cluster. "
                                    + "These archives can come from the local path of flink client or HDFS. "
                                    + "They will be un-packed when localizing and they can be any of the following "
                                    + "types: \".tar.gz\", \".tar\", \".tgz\", \".dst\", \".jar\", \".zip\". "
                                    + "For example, \"/path/to/local/archive.jar;"
                                    + "hdfs://$namenode_address/path/to/archive.jar\"");

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

    public static final ConfigOption<String> TASK_MANAGER_NODE_LABEL =
            key("yarn.taskmanager.node-label")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify YARN node label for the Flink TaskManagers, it will "
                                    + "override the yarn.application.node-label for TaskManagers if both are set.");

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
                                    + "true, Flink will ship the keytab file as a YARN local "
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

    /**
     * Allows users to directly utilize usrlib directory in HDFS for YARN application mode. The
     * classloader for loading jars under the usrlib will be controlled by {@link
     * YarnConfigOptions#CLASSPATH_INCLUDE_USER_JAR}.
     */
    public static final ConfigOption<String> PROVIDED_USRLIB_DIR =
            key("yarn.provided.usrlib.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The provided usrlib directory in remote. It should be pre-uploaded and "
                                    + "world-readable. Flink will use it to exclude the local usrlib directory(i.e. usrlib/ under the parent directory of FLINK_LIB_DIR)."
                                    + " Unlike yarn.provided.lib.dirs, YARN will not cache it on the nodes as it is for each application. An example could be "
                                    + "hdfs://$namenode_address/path/of/flink/usrlib");

    @SuppressWarnings("unused")
    public static final ConfigOption<String> HADOOP_CONFIG_KEY =
            key("flink.hadoop.<key>")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "A general option to probe Hadoop configuration through prefix 'flink.hadoop.'. Flink will remove the prefix to get <key> (from %s and %s) then set the <key> and value to Hadoop configuration."
                                                    + " For example, flink.hadoop.dfs.replication=5 in Flink configuration and convert to dfs.replication=5 in Hadoop configuration.",
                                            link(
                                                    "https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/core-default.xml",
                                                    "core-default.xml"),
                                            link(
                                                    "https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml",
                                                    "hdfs-default.xml"))
                                    .build());

    @SuppressWarnings("unused")
    public static final ConfigOption<String> YARN_CONFIG_KEY =
            key("flink.yarn.<key>")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "A general option to probe Yarn configuration through prefix 'flink.yarn.'. Flink will remove the prefix 'flink.' to get yarn.<key> (from %s) then set the yarn.<key> and value to Yarn configuration."
                                                    + " For example, flink.yarn.resourcemanager.container.liveness-monitor.interval-ms=300000 in Flink configuration and convert to yarn.resourcemanager.container.liveness-monitor.interval-ms=300000 in Yarn configuration.",
                                            link(
                                                    "https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-common/yarn-default.xml",
                                                    "yarn-default.xml"))
                                    .build());

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
    public enum UserJarInclusion implements DescribedEnum {
        DISABLED(text("Exclude user jars from the system class path")),
        FIRST(text("Position at the beginning")),
        LAST(text("Position at the end")),
        ORDER(text("Position based on the name of the jar"));

        private final InlineElement description;

        UserJarInclusion(InlineElement description) {
            this.description = description;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
