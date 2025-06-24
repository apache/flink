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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Simple {@link TriggerTestHarness} that accepts integers and takes the value as the timestamp for
 * the {@link StreamRecord}.
 */
public class SimpleTriggerTestHarness<W extends Window> extends TriggerTestHarness<Integer, W> {

    public SimpleTriggerTestHarness(Trigger<Integer, W> trigger, TypeSerializer<W> windowSerializer)
            throws Exception {
        super(trigger, windowSerializer);
    }

    public TriggerResult processElement(Integer element, W window) throws Exception {
        return super.processElement(new StreamRecord<>(element, element), window);
    }
}
