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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/** The {@link ConfigOption configuration options} relevant for all Executors. */
@PublicEvolving
public class DeploymentOptions {

    public static final ConfigOption<String> TARGET =
            key("execution.target")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The deployment target for the execution. This can take one of the following values "
                                                    + "when calling %s:",
                                            TextElement.code("bin/flink run"))
                                    .list(
                                            text("remote"),
                                            text("local"),
                                            text("yarn-per-job (deprecated)"),
                                            text("yarn-session"),
                                            text("kubernetes-session"))
                                    .text(
                                            "And one of the following values when calling %s:",
                                            TextElement.code("bin/flink run-application"))
                                    .list(text("yarn-application"), text("kubernetes-application"))
                                    .build());

    public static final ConfigOption<Boolean> ATTACHED =
            key("execution.attached")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Specifies if the pipeline is submitted in attached or detached mode.");

    public static final ConfigOption<Boolean> SHUTDOWN_IF_ATTACHED =
            key("execution.shutdown-on-attached-exit")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If the job is submitted in attached mode, perform a best-effort cluster shutdown "
                                    + "when the CLI is terminated abruptly, e.g., in response to a user interrupt, such as typing Ctrl + C.");

    public static final ConfigOption<List<String>> JOB_LISTENERS =
            key("execution.job-listeners")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Custom JobListeners to be registered with the execution environment."
                                    + " The registered listeners cannot have constructors with arguments.");

    public static final ConfigOption<Boolean> SHUTDOWN_ON_APPLICATION_FINISH =
            ConfigOptions.key("execution.shutdown-on-application-finish")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether a Flink Application cluster should shut down automatically after its application finishes"
                                    + " (either successfully or as result of a failure). Has no effect for other deployment modes.");

    @Experimental
    public static final ConfigOption<Boolean> SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR =
            ConfigOptions.key("execution.submit-failed-job-on-application-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If a failed job should be submitted (in the application mode) when"
                                                    + " there is an error in the application driver before an actual job"
                                                    + " submission. This is intended for providing a clean way of reporting"
                                                    + " failures back to the user and is especially useful in combination"
                                                    + " with '%s'. This option only works when the single job submission is"
                                                    + " enforced ('%s' is enabled). Please note that this is an experimental"
                                                    + " option and may be changed in the future.",
                                            TextElement.text(SHUTDOWN_ON_APPLICATION_FINISH.key()),
                                            TextElement.text(HighAvailabilityOptions.HA_MODE.key()))
                                    .build());

    @Experimental
    public static final ConfigOption<Boolean> ALLOW_CLIENT_JOB_CONFIGURATIONS =
            ConfigOptions.key("execution.allow-client-job-configurations")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Determines whether configurations in the user program are "
                                    + "allowed. Depending on your deployment mode failing the job "
                                    + "might have different affects. Either your client that is "
                                    + "trying to submit the job to an external cluster (session "
                                    + "cluster deployment) throws the exception or the Job "
                                    + "manager (application mode deployment).");
}
