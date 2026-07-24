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

package org.apache.beam.runners.fnexecution.control;

import org.apache.flink.configuration.GlobalConfiguration;

import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ProcessPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultJobBundleFactory}. */
class DefaultJobBundleFactoryTest {

    @Test
    void getEnvironmentForLoggingHidesSensitiveProcessEnvironmentVariables() {
        ProcessPayload processPayload =
                ProcessPayload.newBuilder()
                        .setCommand("python")
                        .putEnv("PATH", "/usr/bin")
                        .putEnv("ACCESS_TOKEN", "secret-token")
                        .putEnv("MY_PASSWORD", "secret-password")
                        .build();
        Environment environment =
                Environment.newBuilder()
                        .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS))
                        .setPayload(processPayload.toByteString())
                        .build();

        String environmentForLogging =
                DefaultJobBundleFactory.getEnvironmentForLogging(environment);

        assertThat(environmentForLogging)
                .contains("PATH")
                .contains("/usr/bin")
                .contains("ACCESS_TOKEN")
                .contains("MY_PASSWORD")
                .contains(GlobalConfiguration.HIDDEN_CONTENT)
                .doesNotContain("secret-token")
                .doesNotContain("secret-password");
    }

    @Test
    void getEnvironmentForLoggingHidesAdditionalSensitiveProcessEnvironmentVariables() {
        ProcessPayload processPayload =
                ProcessPayload.newBuilder()
                        .setCommand("python")
                        .putEnv("PATH", "/usr/bin")
                        .putEnv("CUSTOMER_ID", "secret-customer")
                        .build();
        Environment environment =
                Environment.newBuilder()
                        .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS))
                        .setPayload(processPayload.toByteString())
                        .build();

        String environmentForLogging =
                DefaultJobBundleFactory.getEnvironmentForLogging(
                        environment, Collections.singletonList("customer_id"));

        assertThat(environmentForLogging)
                .contains("PATH")
                .contains("/usr/bin")
                .contains("CUSTOMER_ID")
                .contains(GlobalConfiguration.HIDDEN_CONTENT)
                .doesNotContain("secret-customer");
    }

    @Test
    void getEnvironmentForLoggingOmitsMalformedProcessPayload() {
        Environment environment =
                Environment.newBuilder()
                        .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS))
                        .setPayload(ByteString.copyFromUtf8("secret-token"))
                        .build();

        String environmentForLogging =
                DefaultJobBundleFactory.getEnvironmentForLogging(environment);

        assertThat(environmentForLogging).doesNotContain("secret-token").doesNotContain("payload");
    }
}
