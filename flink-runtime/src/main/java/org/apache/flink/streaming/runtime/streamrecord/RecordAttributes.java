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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Experimental;

import java.util.Collections;
import java.util.Objects;

/**
 * A RecordAttributes describes the attributes of records from the current RecordAttributes until
 * the next one is received. It provides stream task with information that can be used to optimize
 * the stream task's performance.
 */
@Experimental
public class RecordAttributes extends StreamElement {

    public static final RecordAttributes EMPTY_RECORD_ATTRIBUTES =
            new RecordAttributesBuilder(Collections.emptyList()).build();
    private final boolean isBacklog;

    public RecordAttributes(boolean isBacklog) {
        this.isBacklog = isBacklog;
    }

    /**
     * If it returns true, then the records received after this element are stale and an operator
     * can optionally buffer records until isBacklog=false. This allows an operator to optimize
     * throughput at the cost of processing latency.
     */
    public boolean isBacklog() {
        return isBacklog;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordAttributes that = (RecordAttributes) o;
        return isBacklog == that.isBacklog;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isBacklog);
    }

    @Override
    public String toString() {
        return "RecordAttributes{" + "backlog=" + isBacklog + '}';
    }
}
