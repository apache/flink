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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests the behavior of the {@link OperatorState}. */
public class OperatorStateTest {

    @Test
    public void testFullyFinishedOperatorState() {
        OperatorState operatorState = new OperatorState(new OperatorID(), 5, 256);
        assertFalse(operatorState.isFullyFinished());

        operatorState.putState(0, OperatorSubtaskState.builder().build());
        assertFalse(operatorState.isFullyFinished());

        operatorState.markedFullyFinished();
        assertTrue(operatorState.isFullyFinished());

        try {
            operatorState.putState(1, OperatorSubtaskState.builder().build());
            fail("Should not be able to put new subtask states for a fully finished state");
        } catch (IllegalStateException e) {
            // Expected
        }
    }
}
