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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/** {@link org.apache.flink.runtime.testtasks.BlockingNoOpInvokable} that sometimes fails. */
public class SometimesExceptionSender extends AbstractInvokable {

    private static Set<Integer> failingSenders;

    public SometimesExceptionSender(Environment environment) {
        super(environment);
    }

    static void configFailingSenders(int numTasks) {
        failingSenders = Collections.singleton(ThreadLocalRandom.current().nextInt(numTasks));
    }

    @Override
    public void invoke() throws Exception {
        if (failingSenders.contains(this.getIndexInSubtaskGroup())) {
            throw new Exception("Test exception");
        } else {
            final Object o = new Object();
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (o) {
                //noinspection InfiniteLoopStatement
                while (true) {
                    o.wait();
                }
            }
        }
    }
}
