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

package org.apache.flink.runtime.state.heap;

/** Head level index for skip list. */
public interface LevelIndexHeader {

    /**
     * Returns the top level of skip list.
     *
     * @return the top level of skip list.
     */
    int getLevel();

    /**
     * Updates the top level of skip list to the given level.
     *
     * @param level the level which top level of skip list updates to.
     */
    void updateLevel(int level);

    /**
     * Returns the next node in the given level.
     *
     * @param level the level whose next node is returned.
     * @return id of the next node.
     */
    long getNextNode(int level);

    /**
     * Updates the next node in the given level to the specified node id.
     *
     * @param level the level whose next node is updated.
     * @param newNodeId the id of the next node.
     */
    void updateNextNode(int level, long newNodeId);
}
