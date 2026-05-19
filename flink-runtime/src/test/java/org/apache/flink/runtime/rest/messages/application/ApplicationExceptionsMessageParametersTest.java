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

package org.apache.flink.runtime.rest.messages.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.rest.messages.MessageParameters;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ApplicationExceptionsMessageParameters}. */
class ApplicationExceptionsMessageParametersTest {

    @Test
    void testMaxExceptionsQueryParameterIsRendered() throws Exception {
        ApplicationID applicationId = ApplicationID.generate();
        ApplicationExceptionsMessageParameters parameters =
                new ApplicationExceptionsMessageParameters();
        parameters.applicationPathParameter.resolve(applicationId);
        parameters.upperLimitExceptionParameter.resolveFromString("20");

        String resolvedUrl =
                MessageParameters.resolveUrl(ApplicationExceptionsHeaders.URL, parameters);

        assertThat(resolvedUrl)
                .isEqualTo("/applications/" + applicationId + "/exceptions?maxExceptions=20");
    }

    @Test
    void testMaxExceptionsQueryParameterIsOmittedWhenUnresolved() {
        ApplicationID applicationId = ApplicationID.generate();
        ApplicationExceptionsMessageParameters parameters =
                new ApplicationExceptionsMessageParameters();
        parameters.applicationPathParameter.resolve(applicationId);

        String resolvedUrl =
                MessageParameters.resolveUrl(ApplicationExceptionsHeaders.URL, parameters);

        assertThat(resolvedUrl).isEqualTo("/applications/" + applicationId + "/exceptions");
    }
}
