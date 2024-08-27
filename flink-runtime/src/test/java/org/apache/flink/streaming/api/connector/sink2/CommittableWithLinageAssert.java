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

import org.assertj.core.api.AbstractAssert;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Custom assertions for {@link
 * org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage}.
 */
public class CommittableWithLinageAssert
        extends AbstractAssert<CommittableWithLinageAssert, CommittableWithLineage<?>> {

    public CommittableWithLinageAssert(CommittableWithLineage<?> summary) {
        super(summary, CommittableWithLinageAssert.class);
    }

    public CommittableWithLinageAssert isEqualTo(CommittableWithLineage<?> committableWithLineage) {
        isNotNull();
        assertThat(actual.getSubtaskId()).isEqualTo(committableWithLineage.getSubtaskId());
        assertThat(actual.getCheckpointId()).isEqualTo(committableWithLineage.getCheckpointId());
        assertThat(actual.getCommittable()).isEqualTo(committableWithLineage.getCommittable());
        return this;
    }

    public CommittableWithLinageAssert hasCommittable(Object committable) {
        isNotNull();
        assertThat(actual.getCommittable()).isEqualTo(committable);
        return this;
    }

    public CommittableWithLinageAssert hasCheckpointId(@Nullable Long checkpointId) {
        isNotNull();
        if (checkpointId == null) {
            assertThat(actual.getCheckpointId()).isEmpty();
        } else {
            assertThat(actual.getCheckpointId()).hasValue(checkpointId);
        }
        return this;
    }

    public CommittableWithLinageAssert hasSubtaskId(int subtaskId) {
        isNotNull();
        assertThat(actual.getSubtaskId()).isEqualTo(subtaskId);
        return this;
    }
}
