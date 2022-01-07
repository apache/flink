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

import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7Split;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** Tests for {@link Elasticsearch7SourceEnumStateSerializer}. */
public class Elasticsearch7SourceEnumStateSerializerTest {
    private static final String PIT_ID = "27W0AwEIbXktaW5kZXgWbTRmVFpZUjVUSGlZ";
    private static final int NUM_SPLITS = 10;

    @Test
    public void testEnumStateSerde() throws IOException {
        final Elasticsearch7SourceEnumState state =
                new Elasticsearch7SourceEnumState(generateElasticsearchSplits());
        final Elasticsearch7SourceEnumStateSerializer serializer =
                new Elasticsearch7SourceEnumStateSerializer();

        final byte[] bytes = serializer.serialize(state);

        final Elasticsearch7SourceEnumState restoredState =
                serializer.deserialize(serializer.getVersion(), bytes);

        Assertions.assertThat(restoredState.getAssignedSplits())
                .containsExactlyInAnyOrderElementsOf(state.getAssignedSplits());
    }

    private Set<Elasticsearch7Split> generateElasticsearchSplits() {
        Set<Elasticsearch7Split> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new Elasticsearch7Split(PIT_ID, i));
        }
        return splits;
    }
}
