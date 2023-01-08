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

package org.apache.flink.tools.ci.utils.deploy;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DeployParserTest {
    @Test
    void testParseDeployOutputDetectsDeployment() {
        assertThat(
                        DeployParser.parseDeployOutput(
                                Stream.of(
                                        "[INFO] --- maven-deploy-plugin:2.8.2:deploy (default-deploy) @ flink-parent ---",
                                        "[INFO] ")))
                .containsExactly("flink-parent");
    }

    @Test
    void testParseDeployOutputDetectsDeploymentWithAltRepository() {
        assertThat(
                        DeployParser.parseDeployOutput(
                                Stream.of(
                                        "[INFO] --- maven-deploy-plugin:2.8.2:deploy (default-deploy) @ flink-parent ---",
                                        "[INFO] Using alternate deployment repository.../tmp/flink-validation-deployment")))
                .containsExactly("flink-parent");
    }

    @Test
    void testParseDeployOutputDetectsSkippedDeployments() {
        assertThat(
                        DeployParser.parseDeployOutput(
                                Stream.of(
                                        "[INFO] --- maven-deploy-plugin:2.8.2:deploy (default-deploy) @ flink-parent ---",
                                        "[INFO] Skipping artifact deployment")))
                .isEmpty();
    }
}
