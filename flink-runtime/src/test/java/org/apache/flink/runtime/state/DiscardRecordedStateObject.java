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

package org.apache.flink.runtime.state;

import org.apache.flink.util.TernaryBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** A test mock of {@link StateObject} which need record itself whether been discarded. */
public interface DiscardRecordedStateObject extends StateObject {

    boolean isDisposed();

    static void verifyDiscard(StateObject stateHandle, TernaryBoolean expected) {
        if (stateHandle instanceof DiscardRecordedStateObject) {
            DiscardRecordedStateObject testingHandle = (DiscardRecordedStateObject) stateHandle;
            if (expected.getAsBoolean() != null) {
                assertThat(testingHandle.isDisposed()).isEqualTo(expected.getAsBoolean());
            }
        } else {
            throw new IllegalStateException("stateHandle must be DiscardRecordedStateObject !");
        }
    }
}
