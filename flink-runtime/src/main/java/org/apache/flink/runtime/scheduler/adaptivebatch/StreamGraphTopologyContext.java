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

package org.apache.flink.runtime.scheduler.adaptivebatch;

/** Interface for retrieving stream graph context details for adaptive batch jobs. */
public interface StreamGraphTopologyContext {

    /**
     * Retrieves the parallelism of a given stream node.
     *
     * @param streamNodeId the ID of the stream node.
     * @return the current parallelism of the stream node.
     */
    int getParallelism(int streamNodeId);

    /**
     * Retrieves the maximum parallelism for a specified stream node. If the maximum parallelism is
     * not explicitly set, the default maximum parallelism is returned.
     *
     * @param streamNodeId the ID of the stream node.
     * @return the maximum parallelism for the stream node, or the default if unspecified.
     */
    int getMaxParallelismOrDefault(int streamNodeId);

    /**
     * Retrieves the count of pending operators waiting to be transferred to job vertices.
     *
     * @return the number of pending operators.
     */
    int getPendingOperatorCount();
}
