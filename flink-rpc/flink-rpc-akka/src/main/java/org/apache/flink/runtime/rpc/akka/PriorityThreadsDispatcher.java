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

package org.apache.flink.runtime.rpc.akka;

import akka.dispatch.DefaultDispatcherPrerequisites;
import akka.dispatch.DispatcherConfigurator;
import akka.dispatch.DispatcherPrerequisites;
import com.typesafe.config.Config;

/**
 * Akka dispatcher threads creates threads with configurable priority.
 *
 * <p>Example of configuration:
 *
 * <pre>
 *
 *   low-priority-threads-dispatcher {
 *     type = org.apache.flink.runtime.rpc.akka.PriorityThreadsDispatcher
 *     executor = "thread-pool-executor"
 *     # should be between Thread.MIN_PRIORITY (which is 1) and Thread.MAX_PRIORITY (which is 10)
 *     threads-priority = 1
 *     thread-pool-executor {
 *       core-pool-size-min = 0
 *       core-pool-size-factor = 2.0
 *       core-pool-size-max = 10
 *     }
 *   }
 * </pre>
 *
 * <p>Two arguments constructor (the primary constructor) is automatically called by Akka when it
 * finds:
 *
 * <pre>
 *   abcde-dispatcher {
 *     type = org.apache.flink.runtime.rpc.akka.PriorityThreadsDispatcher <-- the class that Akka will instantiate
 *     ...
 *   }
 * </pre>
 */
public class PriorityThreadsDispatcher extends DispatcherConfigurator {
    /**
     * @param config passed automatically by Akka, should contain information about threads priority
     * @param prerequisites passed automatically by Akka
     */
    public PriorityThreadsDispatcher(Config config, DispatcherPrerequisites prerequisites) {
        super(
                config,
                createPriorityThreadDispatcherPrerequisites(
                        prerequisites, config.getInt("thread-priority")));
    }

    private static DispatcherPrerequisites createPriorityThreadDispatcherPrerequisites(
            DispatcherPrerequisites prerequisites, int newThreadPriority) {
        return new DefaultDispatcherPrerequisites(
                new PrioritySettingThreadFactory(prerequisites.threadFactory(), newThreadPriority),
                prerequisites.eventStream(),
                prerequisites.scheduler(),
                prerequisites.dynamicAccess(),
                prerequisites.settings(),
                prerequisites.mailboxes(),
                prerequisites.defaultExecutionContext());
    }
}
