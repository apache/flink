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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;

import java.util.concurrent.Executor;

/** Factory for job status changed listener. */
@PublicEvolving
public interface JobStatusChangedListenerFactory {

    JobStatusChangedListener createListener(Context context);

    @PublicEvolving
    interface Context {
        /*
         * Configuration for the factory to create listener, users can add customized options to flink and get them here to create the listener. For
         * example, users can add rest address for datahub to the configuration, and get it when they need to create http client for the listener.
         */
        Configuration getConfiguration();

        /**
         * User classloader for the flink application.
         *
         * @return
         */
        ClassLoader getUserClassLoader();

        /*
         * Get an Executor pool for the listener to run async operations that can potentially be IO-heavy. `JobMaster` will provide an independent executor
         * for io operations and it won't block the main-thread. All tasks submitted to the executor will be executed in parallel, and when the job ends,
         * previously submitted tasks will be executed, but no new tasks will be accepted.
         */
        Executor getIOExecutor();
    }
}
