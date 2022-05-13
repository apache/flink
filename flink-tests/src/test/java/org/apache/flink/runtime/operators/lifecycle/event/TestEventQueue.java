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

import org.apache.flink.testutils.junit.SharedObjects;

import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;

/**
 * A queue for {@link TestEvent}. The purpose is to allow to react to operator lifecycle events
 * externally. Events are not collected in the job output (e.g. accumulators) because they can be
 * emitted when the outputs are closed.
 */
public interface TestEventQueue extends Serializable {
    void add(TestEvent e);

    List<TestEvent> getAll();

    void withHandler(TestEventHandler handler) throws Exception;

    void addListener(Consumer<TestEvent> listener);

    void removeListener(Consumer<TestEvent> listener);

    /** Handler of {@link TestEvent}s. */
    interface TestEventHandler {
        enum TestEventNextAction {
            CONTINUE,
            STOP
        }

        TestEventNextAction handle(TestEvent e);
    }

    static TestEventQueue createShared(SharedObjects sharedObjects) {
        return new SharedTestEventQueue(sharedObjects.add(new TestEventQueueImpl()));
    }
}
