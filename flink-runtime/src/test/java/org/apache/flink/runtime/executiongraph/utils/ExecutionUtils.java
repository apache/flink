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
 *
 */

package org.apache.flink.runtime.executiongraph.utils;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;

public final class ExecutionUtils {

    private ExecutionUtils() {
        // Private constructor for utility class.
    }

    /**
     * Waits for the task deployment descriptors to be created for the given execution vertices.
     *
     * @param executionVertices the execution vertices to wait for
     */
    public static void waitForTaskDeploymentDescriptorsCreation(
            final ExecutionVertex... executionVertices) {
        for (ExecutionVertex ev : executionVertices) {
            ev.getTddCreationDuringDeployFuture().join();
        }
    }
}
