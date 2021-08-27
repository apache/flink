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

package org.apache.flink.runtime.operators.coordination;

/** A dummy/mock implementation of an {@link OperatorEvent}. */
public final class TestOperatorEvent implements OperatorEvent {
    private static final long serialVersionUID = 1L;

    private final int value;

    public TestOperatorEvent() {
        // pick some random and rather unique value
        this.value = System.identityHashCode(this);
    }

    public TestOperatorEvent(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == TestOperatorEvent.class
                        && ((TestOperatorEvent) obj).value == this.value);
    }

    @Override
    public String toString() {
        return "TestOperatorEvent (" + value + ')';
    }
}
