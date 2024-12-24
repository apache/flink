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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class represents the information about the finished operators. It includes a list of
 * StreamNode IDs representing the finished operators, and a map associating each finished
 * StreamNode ID with their corresponding produced data size and distribution information.
 */
public class OperatorsFinished {

    /** A list that holds the IDs of the finished StreamNodes. */
    private final List<Integer> finishedStreamNodeIds;

    /**
     * A map that associates each finished StreamNode ID with a list of IntermediateResultInfo
     * objects. The key is the StreamNode ID, and the value is a list of IntermediateResultInfo.
     */
    private final Map<Integer, List<BlockingResultInfo>> resultInfoMap;

    public OperatorsFinished(
            List<Integer> finishedStreamNodeIds,
            Map<Integer, List<BlockingResultInfo>> resultInfoMap) {
        this.finishedStreamNodeIds = checkNotNull(finishedStreamNodeIds);
        this.resultInfoMap = checkNotNull(resultInfoMap);
    }

    public List<Integer> getFinishedStreamNodeIds() {
        return Collections.unmodifiableList(finishedStreamNodeIds);
    }

    public Map<Integer, List<BlockingResultInfo>> getResultInfoMap() {
        return Collections.unmodifiableMap(resultInfoMap);
    }
}
