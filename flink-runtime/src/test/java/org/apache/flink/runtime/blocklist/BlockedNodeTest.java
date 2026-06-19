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

package org.apache.flink.runtime.blocklist;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link BlockedNode}. */
class BlockedNodeTest {

    @Test
    void testEquals() {
        BlockedNode blockedNode1 = new BlockedNode("node", "cause", 1L);
        BlockedNode blockedNode2 = new BlockedNode("node", "cause", 1L);
        assertThat(blockedNode1).isEqualTo(blockedNode2);
    }

    @Test
    void testNotEquals() {
        BlockedNode blockedNode1 = new BlockedNode("node1", "cause1", 1L);
        BlockedNode blockedNode2 = new BlockedNode("node2", "cause1", 1L);
        BlockedNode blockedNode3 = new BlockedNode("node1", "cause2", 1L);
        BlockedNode blockedNode4 = new BlockedNode("node1", "cause1", 2L);

        assertThat(blockedNode1).isNotEqualTo(blockedNode2);
        assertThat(blockedNode1).isNotEqualTo(blockedNode3);
        assertThat(blockedNode1).isNotEqualTo(blockedNode4);
    }
}
