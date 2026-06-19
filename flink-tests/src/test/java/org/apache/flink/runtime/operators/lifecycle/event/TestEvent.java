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

package org.apache.flink.runtime.operators.lifecycle.event;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.Objects;

/**
 * An event of calling some {@link org.apache.flink.streaming.api.operators.StreamOperator} method
 * or some other event in the operator lifecycle.
 */
@Internal
public abstract class TestEvent implements Serializable {
    public final String operatorId;
    public final int subtaskIndex;
    public final int attemptNumber;
    private static final long serialVersionUID = 1L;

    TestEvent(String operatorId, int subtaskIndex, int attemptNumber) {
        this.operatorId = operatorId;
        this.subtaskIndex = subtaskIndex;
        this.attemptNumber = attemptNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TestEvent)) {
            return false;
        }
        TestEvent testEvent = (TestEvent) o;
        return subtaskIndex == testEvent.subtaskIndex && operatorId.equals(testEvent.operatorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorId, subtaskIndex);
    }

    @Override
    public String toString() {
        return String.format(
                "%s %s/%d",
                getClass().getSimpleName().replaceAll("Event\\b", ""), operatorId, subtaskIndex);
    }
}
