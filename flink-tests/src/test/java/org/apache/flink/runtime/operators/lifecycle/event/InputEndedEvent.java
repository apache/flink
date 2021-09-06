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
import org.apache.flink.streaming.api.operators.BoundedMultiInput;

import java.util.Objects;

/**
 * An event of calling {@link BoundedMultiInput#endInput(int) endInput} method or the like on an
 * operator.
 */
@Internal
public class InputEndedEvent extends TestEvent {
    public final int inputId;

    public InputEndedEvent(String operatorId, int subtaskIndex, int attemptNumber, int inputId) {
        super(operatorId, subtaskIndex, attemptNumber);
        this.inputId = inputId;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof InputEndedEvent)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        InputEndedEvent that = (InputEndedEvent) o;
        return inputId == that.inputId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inputId);
    }

    @Override
    public String toString() {
        return super.toString() + "(" + inputId + ")";
    }
}
