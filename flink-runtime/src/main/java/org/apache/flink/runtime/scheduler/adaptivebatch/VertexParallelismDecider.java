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

import java.util.List;

/**
 * {@link VertexParallelismDecider} is responsible for determining the parallelism of a job vertex,
 * based on the size of the consumed blocking results.
 */
public interface VertexParallelismDecider {

    /**
     * Computing the parallelism.
     *
     * @param consumedResults The information of consumed blocking results.
     * @return the parallelism of the job vertex.
     */
    int decideParallelismForVertex(List<BlockingResultInfo> consumedResults);
}
