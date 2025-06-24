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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;

/** Configuration options for the WebMonitorEndpoint. */
@PublicEvolving
public class WebOptions {

    /**
     * The config parameter defining the Access-Control-Allow-Origin header for all responses from
     * the web-frontend.
     */
    public static final ConfigOption<String> ACCESS_CONTROL_ALLOW_ORIGIN =
            key("web.access-control-allow-origin")
                    .stringType()
                    .defaultValue("*")
                    .withDeprecatedKeys("jobmanager.web.access-control-allow-origin")
                    .withDescription(
                            "Access-Control-Allow-Origin header for all responses from the web-frontend.");

    /** The config parameter defining the refresh interval for the web-frontend in milliseconds. */
    public static final ConfigOption<Duration> REFRESH_INTERVAL =
            key("web.refresh-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(3000L))
                    .withDeprecatedKeys("jobmanager.web.refresh-interval")
                    .withDescription("Refresh interval for the web-frontend.");

    /** The config parameter defining the flink web directory to be used by the webmonitor. */
    @Documentation.OverrideDefault("System.getProperty(\"java.io.tmpdir\")")
    public static final ConfigOption<String> TMP_DIR =
            key("web.tmpdir")
                    .stringType()
                    .defaultValue(System.getProperty("java.io.tmpdir"))
                    .withDeprecatedKeys("jobmanager.web.tmpdir")
                    .withDescription(
                            "Local directory that is used by the REST API for temporary files.");

    /**
     * The config parameter defining the directory for uploading the job jars. If not specified a
     * dynamic directory will be used under the directory specified by JOB_MANAGER_WEB_TMPDIR_KEY.
     */
    public static final ConfigOption<String> UPLOAD_DIR =
            key("web.upload.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("jobmanager.web.upload.dir")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Local directory that is used by the REST API for storing uploaded jars. If not specified a dynamic directory will be created"
                                                    + " under %s.",
                                            code(TMP_DIR.key()))
                                    .build());

    /** The config parameter defining the number of archived jobs for the JobManager. */
    public static final ConfigOption<Integer> ARCHIVE_COUNT =
            key("web.history")
                    .intType()
                    .defaultValue(5)
                    .withDeprecatedKeys("jobmanager.web.history")
                    .withDescription("Number of archived jobs for the JobManager.");

    /**
     * The log file location (may be in /log for standalone but under log directory when using
     * YARN).
     */
    public static final ConfigOption<String> LOG_PATH =
            key("web.log.path")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("jobmanager.web.log.path")
                    .withDescription(
                            "Path to the log file (may be in /log for standalone but under log directory when using YARN).");

    /** Config parameter indicating whether jobs can be uploaded and run from the web-frontend. */
    public static final ConfigOption<Boolean> SUBMIT_ENABLE =
            key("web.submit.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys("jobmanager.web.submit.enable")
                    .withDescription(
                            "Flag indicating whether jobs can be uploaded and run from the web-frontend.");

    /** Config parameter indicating whether jobs can be canceled from the web-frontend. */
    public static final ConfigOption<Boolean> CANCEL_ENABLE =
            key("web.cancel.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Flag indicating whether jobs can be canceled from the web-frontend.");

    /** Config parameter indicating whether jobs can be rescaled from the web-frontend. */
    public static final ConfigOption<Boolean> RESCALE_ENABLE =
            key("web.rescale.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Flag indicating whether jobs can be rescaled from the web-frontend.");

    /** Config parameter defining the number of checkpoints to remember for recent history. */
    public static final ConfigOption<Integer> CHECKPOINTS_HISTORY_SIZE =
            key("web.checkpoints.history")
                    .intType()
                    .defaultValue(10)
                    .withDeprecatedKeys("jobmanager.web.checkpoints.history")
                    .withDescription("Number of checkpoints to remember for recent history.");

    /** The maximum number of failures kept in the exception history. */
    // the parameter is referenced in the UI and might need to be updated there as well
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<Integer> MAX_EXCEPTION_HISTORY_SIZE =
            key("web.exception-history-size")
                    .intType()
                    .defaultValue(16)
                    .withDescription(
                            "The maximum number of failures collected by the exception history per job.");

    /** Timeout for asynchronous operations by the web monitor in milliseconds. */
    public static final ConfigOption<Duration> TIMEOUT =
            key("web.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(10L * 60L * 1000L))
                    .withDescription("Timeout for asynchronous operations by the web monitor.");

    // ------------------------------------------------------------------------

    /** Not meant to be instantiated. */
    private WebOptions() {}
}
