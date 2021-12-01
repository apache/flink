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

package org.apache.flink.connector.elasticsearch.source.enumerator;

import org.apache.flink.connector.elasticsearch.source.split.ElasticsearchSplit;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** Tests for {@link ElasticsearchEnumStateSerializer}. */
public class ElasticsearchEnumStateSerializerTest {
    private static final String PIT_ID = "27W0AwEIbXktaW5kZXgWbTRmVFpZUjVUSGlZ";
    private static final int NUM_SPLITS = 10;

    @Test
    public void testEnumStateSerde() throws IOException {
        final ElasticsearchEnumState state =
                new ElasticsearchEnumState(generateElasticsearchSplits());
        final ElasticsearchEnumStateSerializer serializer = new ElasticsearchEnumStateSerializer();

        final byte[] bytes = serializer.serialize(state);

        final ElasticsearchEnumState restoredState =
                serializer.deserialize(serializer.getVersion(), bytes);

        Assertions.assertThat(restoredState.getAssignedSplits())
                .containsExactlyInAnyOrderElementsOf(state.getAssignedSplits());
    }

    private Set<ElasticsearchSplit> generateElasticsearchSplits() {
        Set<ElasticsearchSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new ElasticsearchSplit(PIT_ID, i));
        }
        return splits;
    }
}
