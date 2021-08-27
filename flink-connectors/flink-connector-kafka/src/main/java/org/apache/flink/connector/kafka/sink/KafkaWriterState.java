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

package org.apache.flink.connector.kafka.sink;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

class KafkaWriterState {
    private final int subtaskId;
    private final long transactionalIdOffset;
    private final String transactionalIdPrefix;

    KafkaWriterState(
            String transactionalIdPrefix, int subtaskId, long nextFreeTransactionalIdOffset) {
        this.subtaskId = subtaskId;
        this.transactionalIdOffset = nextFreeTransactionalIdOffset;
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public long getTransactionalIdOffset() {
        return transactionalIdOffset;
    }

    public String getTransactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaWriterState that = (KafkaWriterState) o;
        return subtaskId == that.subtaskId && transactionalIdOffset == that.transactionalIdOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtaskId, transactionalIdOffset);
    }

    @Override
    public String toString() {
        return "KafkaWriterState{"
                + "subtaskId="
                + subtaskId
                + ", transactionalIdOffset="
                + transactionalIdOffset
                + ", transactionalIdPrefix='"
                + transactionalIdPrefix
                + '\''
                + '}';
    }
}
