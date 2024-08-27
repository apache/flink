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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RecordAttributesCombiner}. */
class RecordAttributesCombinerTest {

    @Test
    void testCombineRecordAttributes() throws Exception {
        final RecordAttributesCombiner combiner = new RecordAttributesCombiner(3);
        CollectingDataOutput<Object> collectingDataOutput = new CollectingDataOutput<>();
        final RecordAttributes backlogRecordAttribute =
                new RecordAttributesBuilder(Collections.emptyList()).setBacklog(true).build();
        final RecordAttributes nonBacklogRecordAttribute =
                new RecordAttributesBuilder(Collections.emptyList()).setBacklog(false).build();

        // Switch from null to backlog
        combiner.inputRecordAttributes(backlogRecordAttribute, 0, collectingDataOutput);
        combiner.inputRecordAttributes(nonBacklogRecordAttribute, 0, collectingDataOutput);
        combiner.inputRecordAttributes(nonBacklogRecordAttribute, 0, collectingDataOutput);

        combiner.inputRecordAttributes(backlogRecordAttribute, 1, collectingDataOutput);
        combiner.inputRecordAttributes(backlogRecordAttribute, 2, collectingDataOutput);

        // Switch from backlog to non-backlog
        combiner.inputRecordAttributes(nonBacklogRecordAttribute, 1, collectingDataOutput);
        combiner.inputRecordAttributes(nonBacklogRecordAttribute, 2, collectingDataOutput);

        // Switch from non-backlog to backlog
        combiner.inputRecordAttributes(backlogRecordAttribute, 1, collectingDataOutput);

        assertThat(collectingDataOutput.getEvents())
                .containsExactly(
                        backlogRecordAttribute, nonBacklogRecordAttribute, backlogRecordAttribute);
    }

    @Test
    void testCombinerOnlyOutputNonBacklogWhenAllInputChannelAreNonBacklog() throws Exception {
        final RecordAttributesCombiner combiner = new RecordAttributesCombiner(2);
        CollectingDataOutput<Object> collectingDataOutput = new CollectingDataOutput<>();

        final RecordAttributes backlogRecordAttribute =
                new RecordAttributesBuilder(Collections.emptyList()).setBacklog(true).build();
        final RecordAttributes nonBacklogRecordAttribute =
                new RecordAttributesBuilder(Collections.emptyList()).setBacklog(false).build();

        combiner.inputRecordAttributes(nonBacklogRecordAttribute, 0, collectingDataOutput);
        assertThat(collectingDataOutput.getEvents()).isEmpty();

        combiner.inputRecordAttributes(backlogRecordAttribute, 0, collectingDataOutput);
        combiner.inputRecordAttributes(nonBacklogRecordAttribute, 1, collectingDataOutput);
        assertThat(collectingDataOutput.getEvents()).containsExactly(backlogRecordAttribute);

        combiner.inputRecordAttributes(nonBacklogRecordAttribute, 0, collectingDataOutput);
        assertThat(collectingDataOutput.getEvents())
                .containsExactly(backlogRecordAttribute, nonBacklogRecordAttribute);
    }
}
