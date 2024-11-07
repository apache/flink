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

package org.apache.flink.streaming.api.connector.sink2;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class CommittableMessageSerializerTest {

    private static final CommittableMessageSerializer<Integer> SERIALIZER =
            new CommittableMessageSerializer<>(new IntegerSerializer());

    @Test
    void testCommittableWithLinageSerDe() throws IOException {
        final CommittableWithLineage<Integer> committableWithLineage =
                new CommittableWithLineage<>(1, 2L, 3);
        final CommittableMessage<Integer> message =
                SERIALIZER.deserialize(
                        CommittableMessageSerializer.VERSION,
                        SERIALIZER.serialize(committableWithLineage));
        assertThat(message).isInstanceOf(CommittableWithLineage.class);
        final CommittableWithLineage<Integer> copy = (CommittableWithLineage<Integer>) message;
        assertThat(copy.getCommittable()).isEqualTo(1);
        assertThat(copy.getCheckpointIdOrEOI()).isEqualTo(2L);
        assertThat(copy.getSubtaskId()).isEqualTo(3);
    }

    @Test
    void testCommittableSummarySerDe() throws IOException {
        final CommittableSummary<Integer> committableSummary =
                new CommittableSummary<>(1, 2, 3L, 4, 5);
        final CommittableMessage<Integer> message =
                SERIALIZER.deserialize(
                        CommittableMessageSerializer.VERSION,
                        SERIALIZER.serialize(committableSummary));
        assertThat(message).isInstanceOf(CommittableSummary.class);
        final CommittableSummary<Integer> copy = (CommittableSummary<Integer>) message;
        assertThat(copy.getSubtaskId()).isEqualTo(1);
        assertThat(copy.getNumberOfSubtasks()).isEqualTo(2);
        assertThat(copy.getCheckpointIdOrEOI()).isEqualTo(3L);
        assertThat(copy.getNumberOfCommittables()).isEqualTo(4);
    }
}
