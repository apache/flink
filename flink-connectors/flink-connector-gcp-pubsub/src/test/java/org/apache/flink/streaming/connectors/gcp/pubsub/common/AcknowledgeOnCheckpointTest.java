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

package org.apache.flink.streaming.connectors.gcp.pubsub.common;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Unit tests for {@link AcknowledgeOnCheckpoint}. */
class AcknowledgeOnCheckpointTest {
    private final Acknowledger<String> mockedAcknowledger = mock(Acknowledger.class);

    @Test
    void testRestoreStateAndSnapshot() {
        List<AcknowledgeIdsForCheckpoint<String>> input = new ArrayList<>();
        input.add(new AcknowledgeIdsForCheckpoint<>(0, asList("idsFor0", "moreIdsFor0")));
        input.add(new AcknowledgeIdsForCheckpoint<>(1, asList("idsFor1", "moreIdsFor1")));

        AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint =
                new AcknowledgeOnCheckpoint<>(mockedAcknowledger);
        acknowledgeOnCheckpoint.restoreState(input);

        List<AcknowledgeIdsForCheckpoint<String>> actual =
                acknowledgeOnCheckpoint.snapshotState(2, 100);

        assertThat(actual).hasSize(3);
        assertThat(actual.get(0)).isEqualTo(input.get(0));
        assertThat(actual.get(1)).isEqualTo(input.get(1));
        assertThat(actual.get(2).getCheckpointId()).isEqualTo(2L);
        assertThat(actual.get(2).getAcknowledgeIds()).isEmpty();

        assertThat(acknowledgeOnCheckpoint.numberOfOutstandingAcknowledgements()).isEqualTo(4);
    }

    @Test
    void testAddAcknowledgeIdOnEmptyState() {
        AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint =
                new AcknowledgeOnCheckpoint<>(mockedAcknowledger);

        acknowledgeOnCheckpoint.addAcknowledgeId("ackId");

        List<AcknowledgeIdsForCheckpoint<String>> actual =
                acknowledgeOnCheckpoint.snapshotState(2, 100);

        assertThat(actual.get(0).getCheckpointId()).isEqualTo(2L);
        assertThat(actual.get(0).getAcknowledgeIds()).contains("ackId");

        assertThat(acknowledgeOnCheckpoint.numberOfOutstandingAcknowledgements()).isEqualTo(1);
    }

    @Test
    void testAddAcknowledgeIdOnExistingState() {
        List<AcknowledgeIdsForCheckpoint<String>> input = new ArrayList<>();
        input.add(new AcknowledgeIdsForCheckpoint<>(0, asList("idsFor0", "moreIdsFor0")));
        input.add(new AcknowledgeIdsForCheckpoint<>(1, asList("idsFor1", "moreIdsFor1")));

        AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint =
                new AcknowledgeOnCheckpoint<>(mockedAcknowledger);

        acknowledgeOnCheckpoint.restoreState(input);

        acknowledgeOnCheckpoint.addAcknowledgeId("ackId");

        List<AcknowledgeIdsForCheckpoint<String>> actual =
                acknowledgeOnCheckpoint.snapshotState(94, 100);

        assertThat(actual.get(0)).isEqualTo(input.get(0));
        assertThat(actual.get(1)).isEqualTo(input.get(1));
        assertThat(actual.get(2).getCheckpointId()).isEqualTo(94L);
        assertThat(actual.get(2).getAcknowledgeIds()).contains("ackId");

        assertThat(acknowledgeOnCheckpoint.numberOfOutstandingAcknowledgements()).isEqualTo(5);
    }

    @Test
    void testAddMultipleAcknowledgeIds() {
        AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint =
                new AcknowledgeOnCheckpoint<>(mockedAcknowledger);

        acknowledgeOnCheckpoint.addAcknowledgeId("ackId");
        acknowledgeOnCheckpoint.addAcknowledgeId("ackId2");

        List<AcknowledgeIdsForCheckpoint<String>> actual =
                acknowledgeOnCheckpoint.snapshotState(2, 100);

        assertThat(actual.get(0).getCheckpointId()).isEqualTo(2L);
        assertThat(actual.get(0).getAcknowledgeIds()).contains("ackId", "ackId2");

        assertThat(acknowledgeOnCheckpoint.numberOfOutstandingAcknowledgements()).isEqualTo(2);
    }

    @Test
    void testAcknowledgeIdsForCheckpoint() {
        List<AcknowledgeIdsForCheckpoint<String>> input = new ArrayList<>();
        input.add(new AcknowledgeIdsForCheckpoint<>(0, asList("idsFor0", "moreIdsFor0")));
        input.add(new AcknowledgeIdsForCheckpoint<>(1, asList("idsFor1", "moreIdsFor1")));
        input.add(new AcknowledgeIdsForCheckpoint<>(2, asList("idsFor2", "moreIdsFor2")));
        input.add(new AcknowledgeIdsForCheckpoint<>(3, asList("idsFor3", "moreIdsFor3")));

        AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint =
                new AcknowledgeOnCheckpoint<>(mockedAcknowledger);
        acknowledgeOnCheckpoint.restoreState(input);

        acknowledgeOnCheckpoint.notifyCheckpointComplete(2);

        ArgumentCaptor<List<String>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockedAcknowledger, times(1)).acknowledge(argumentCaptor.capture());
        assertThat(argumentCaptor.getValue())
                .contains(
                        "idsFor0", "moreIdsFor0",
                        "idsFor1", "moreIdsFor1",
                        "idsFor2", "moreIdsFor2");

        assertThat(acknowledgeOnCheckpoint.numberOfOutstandingAcknowledgements()).isEqualTo(2);
    }

    @Test
    void testNumberOfOutstandingAcknowledgementsOnEmptyState() {
        AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint =
                new AcknowledgeOnCheckpoint<>(mockedAcknowledger);
        assertThat(acknowledgeOnCheckpoint.numberOfOutstandingAcknowledgements()).isEqualTo(0);
    }
}
