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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.JobVertex;

/** A simple pojo based implementation of {@link JobVertex.InitializeOnMasterContext}. */
@Internal
public final class SimpleInitializeOnMasterContext implements JobVertex.InitializeOnMasterContext {

    private final ClassLoader loader;
    private final int executionParallelism;

    public SimpleInitializeOnMasterContext(ClassLoader loader, int executionParallelism) {
        this.loader = loader;
        this.executionParallelism = executionParallelism;
    }

    @Override
    public ClassLoader getClassLoader() {
        return loader;
    }

    @Override
    public int getExecutionParallelism() {
        return executionParallelism;
    }
}
