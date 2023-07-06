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

package org.apache.flink.runtime.scheduler;

/**
 * Manages the parallelism properties for a vertex in the execution graph, as well as how they can
 * change during runtime.
 */
public interface VertexParallelismInformation {

    /**
     * Returns a vertex's min parallelism.
     *
     * @return the min parallelism for the vertex
     */
    int getMinParallelism();

    /**
     * Returns a vertex's parallelism.
     *
     * @return the parallelism for the vertex
     */
    int getParallelism();

    /**
     * Returns the vertex's max parallelism.
     *
     * @return the max parallelism for the vertex
     */
    int getMaxParallelism();

    /**
     * Set a given vertex's parallelism property. The parallelism can be changed only if the vertex
     * parallelism was not decided yet (i.e. was -1).
     *
     * @param parallelism the parallelism for the vertex
     */
    void setParallelism(int parallelism);

    /**
     * Changes a given vertex's max parallelism property. The caller should first check the validity
     * of the new setting via {@link #canRescaleMaxParallelism}, otherwise this operation may fail.
     *
     * @param maxParallelism the new max parallelism for the vertex
     */
    void setMaxParallelism(int maxParallelism);

    /**
     * Returns whether the vertex's max parallelism can be changed to a given value.
     *
     * @param desiredMaxParallelism the desired max parallelism for the vertex
     * @return whether the max parallelism can be changed to the given value
     */
    boolean canRescaleMaxParallelism(int desiredMaxParallelism);
}
