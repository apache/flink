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

package org.apache.flink.model.triton;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for inference URL construction in {@link TritonUtils#buildInferenceUrl}. */
class TritonInferenceUrlTest {

    @Test
    void testNullVersionProducesUnversionedUrl() {
        String url = TritonUtils.buildInferenceUrl("http://localhost:8000", "my-model", null);
        assertThat(url).isEqualTo("http://localhost:8000/v2/models/my-model/infer");
    }

    @Test
    void testEmptyVersionProducesUnversionedUrl() {
        String url = TritonUtils.buildInferenceUrl("http://localhost:8000", "my-model", "");
        assertThat(url).isEqualTo("http://localhost:8000/v2/models/my-model/infer");
    }

    @Test
    void testExplicitVersionProducesVersionedUrl() {
        String url = TritonUtils.buildInferenceUrl("http://localhost:8000", "my-model", "1");
        assertThat(url).isEqualTo("http://localhost:8000/v2/models/my-model/versions/1/infer");
    }

    @Test
    void testEndpointWithV2PrefixNullVersion() {
        String url = TritonUtils.buildInferenceUrl("http://localhost:8000/v2", "my-model", null);
        assertThat(url).isEqualTo("http://localhost:8000/v2/models/my-model/infer");
    }

    @Test
    void testEndpointWithV2ModelsNullVersion() {
        String url =
                TritonUtils.buildInferenceUrl("http://localhost:8000/v2/models", "my-model", null);
        assertThat(url).isEqualTo("http://localhost:8000/v2/models/my-model/infer");
    }

    @Test
    void testEndpointWithTrailingSlashNullVersion() {
        String url = TritonUtils.buildInferenceUrl("http://localhost:8000/", "my-model", null);
        assertThat(url).isEqualTo("http://localhost:8000/v2/models/my-model/infer");
    }
}
