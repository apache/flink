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

package org.apache.flink.runtime.operators.lifecycle.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.Input;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An event of sending some data element downstream, usually from {@link Input#processElement
 * processElement} method on an operator.
 */
@Internal
public class TestDataElement {
    public final String operatorId;
    public final int subtaskIndex;
    public final long seq;

    public TestDataElement(String operatorId, int subtaskIndex, long seq) {
        this.operatorId = checkNotNull(operatorId);
        this.subtaskIndex = subtaskIndex;
        this.seq = seq;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TestDataElement)) {
            return false;
        }
        TestDataElement that = (TestDataElement) o;
        return subtaskIndex == that.subtaskIndex
                && seq == that.seq
                && operatorId.equals(that.operatorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorId, subtaskIndex, seq);
    }
}
