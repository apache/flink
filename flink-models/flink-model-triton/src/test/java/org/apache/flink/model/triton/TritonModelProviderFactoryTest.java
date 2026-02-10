/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.triton;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TritonModelProviderFactory}. */
class TritonModelProviderFactoryTest {

    @Test
    void testFactoryIdentifier() {
        TritonModelProviderFactory factory = new TritonModelProviderFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo(TritonModelProviderFactory.IDENTIFIER);
    }

    @Test
    void testRequiredOptions() {
        TritonModelProviderFactory factory = new TritonModelProviderFactory();
        assertThat(factory.requiredOptions())
                .hasSize(2)
                .containsExactlyInAnyOrder(TritonOptions.ENDPOINT, TritonOptions.MODEL_NAME);
    }

    @Test
    void testOptionalOptions() {
        TritonModelProviderFactory factory = new TritonModelProviderFactory();
        assertThat(factory.optionalOptions())
                .hasSize(10)
                .containsExactlyInAnyOrder(
                        TritonOptions.MODEL_VERSION,
                        TritonOptions.TIMEOUT,
                        TritonOptions.FLATTEN_BATCH_DIM,
                        TritonOptions.PRIORITY,
                        TritonOptions.SEQUENCE_ID,
                        TritonOptions.SEQUENCE_START,
                        TritonOptions.SEQUENCE_END,
                        TritonOptions.COMPRESSION,
                        TritonOptions.AUTH_TOKEN,
                        TritonOptions.CUSTOM_HEADERS);
    }
}
