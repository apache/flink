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

import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue.TestEventHandler.TestEventNextAction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import static org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue.TestEventHandler.TestEventNextAction.CONTINUE;

class TestEventQueueImpl implements TestEventQueue {
    private final List<TestEvent> events = new CopyOnWriteArrayList<>();
    private final List<Consumer<TestEvent>> listeners = new CopyOnWriteArrayList<>();

    public void add(TestEvent e) {
        events.add(e);
        listeners.forEach(l -> l.accept(e));
    }

    @Override
    public void withHandler(TestEventHandler handler) throws Exception {
        BlockingQueue<TestEvent> queue = new LinkedBlockingQueue<>();
        Consumer<TestEvent> listener = queue::add;
        addListener(listener);
        try {
            for (TestEventNextAction nextAction = CONTINUE; nextAction == CONTINUE; ) {
                nextAction = handler.handle(queue.take());
            }
        } finally {
            removeListener(listener);
        }
    }

    public void removeListener(Consumer<TestEvent> listener) {
        listeners.remove(listener);
    }

    public void addListener(Consumer<TestEvent> listener) {
        listeners.add(listener);
    }

    @Override
    public List<TestEvent> getAll() {
        return Collections.unmodifiableList(events);
    }
}
