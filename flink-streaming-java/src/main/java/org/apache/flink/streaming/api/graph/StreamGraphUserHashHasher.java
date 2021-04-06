/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * StreamGraphHasher that works with user provided hashes. This is useful in case we want to set
 * (alternative) hashes explicitly, e.g. to provide a way of manual backwards compatibility between
 * versions when the mechanism of generating hashes has changed in an incompatible way.
 */
public class StreamGraphUserHashHasher implements StreamGraphHasher {

    @Override
    public Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph) {
        HashMap<Integer, byte[]> hashResult = new HashMap<>();
        for (StreamNode streamNode : streamGraph.getStreamNodes()) {

            String userHash = streamNode.getUserHash();

            if (null != userHash) {
                hashResult.put(streamNode.getId(), StringUtils.hexStringToByte(userHash));
            }
        }

        return hashResult;
    }
}
