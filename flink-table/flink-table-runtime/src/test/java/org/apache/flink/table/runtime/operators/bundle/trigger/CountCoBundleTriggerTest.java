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

package org.apache.flink.table.runtime.operators.bundle.trigger;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/** Tests for {@link CountCoBundleTrigger}. */
public class CountCoBundleTriggerTest {

    @Test
    public void testTrigger() throws Exception {
        CountCoBundleTrigger<Object, Object> trigger = new CountCoBundleTrigger<>(2);
        TestTriggerCallback callback = new TestTriggerCallback();
        trigger.registerCallback(callback);

        trigger.onElement1(null);
        assertEquals(0, callback.getTriggerCount());

        trigger.onElement2(null);
        assertEquals(1, callback.getTriggerCount());

        trigger.onElement1(null);
        assertEquals(1, callback.getTriggerCount());

        trigger.onElement1(null);
        assertEquals(2, callback.getTriggerCount());

        trigger.onElement2(null);
        assertEquals(2, callback.getTriggerCount());

        trigger.onElement2(null);
        assertEquals(3, callback.getTriggerCount());
    }
}
