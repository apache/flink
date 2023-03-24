/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.blocklist;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultBlocklistTracker}. */
class DefaultBlocklistTrackerTest {

    @Test
    void testAddNewBlockedNodes() {
        BlockedNode blockedNode1 = new BlockedNode("node1", "cause1", 1L);
        BlockedNode blockedNode2 = new BlockedNode("node2", "cause1", 1L);
        BlockedNode blockedNode3 = new BlockedNode("node3", "cause1", 1L);

        BlocklistTracker blocklistTracker = new DefaultBlocklistTracker();

        blocklistTracker.addNewBlockedNodes(
                Arrays.asList(blockedNode1, blockedNode2, blockedNode3));
        assertThat(blocklistTracker.getAllBlockedNodeIds())
                .containsExactlyInAnyOrder("node1", "node2", "node3");

        // should be added
        BlockedNode blockedNode4 = new BlockedNode("node4", "cause1", 1L);
        // replace blockedNode2, because blockedNode5.endTimestamp > blockedNode2.endTimestamp
        BlockedNode blockedNode5 = new BlockedNode("node2", "cause2", 2L);
        // do nothing because blockedNode6 equals to blockedNode3
        BlockedNode blockedNode6 = new BlockedNode("node3", "cause1", 1L);

        BlockedNodeAdditionResult result =
                blocklistTracker.addNewBlockedNodes(
                        Arrays.asList(blockedNode4, blockedNode5, blockedNode6));

        assertThat(result.getNewlyAddedNodes()).containsExactly(blockedNode4);
        assertThat(result.getMergedNodes()).containsExactly(blockedNode5);
        assertThat(blocklistTracker.getAllBlockedNodes())
                .containsExactlyInAnyOrder(blockedNode1, blockedNode3, blockedNode4, blockedNode5);

        // replace blockedNode3, because blockedNode7.endTimestamp == blockedNode3.endTimestamp and
        // blockedNode7.cause != blockedNode3.cause
        BlockedNode blockedNode7 = new BlockedNode("node3", "cause2", 1L);

        result = blocklistTracker.addNewBlockedNodes(Collections.singletonList(blockedNode7));

        assertThat(result.getNewlyAddedNodes()).isEmpty();
        assertThat(result.getMergedNodes()).containsExactly(blockedNode7);
        assertThat(blocklistTracker.getAllBlockedNodes())
                .containsExactlyInAnyOrder(blockedNode1, blockedNode4, blockedNode5, blockedNode7);
    }

    @Test
    void testRemoveTimeoutNodes() {
        BlockedNode blockedNode1 = new BlockedNode("node1", "cause1", 1L);
        BlockedNode blockedNode2 = new BlockedNode("node2", "cause1", 2L);
        BlockedNode blockedNode3 = new BlockedNode("node3", "cause1", 3L);

        BlocklistTracker blocklistTracker = new DefaultBlocklistTracker();

        blocklistTracker.addNewBlockedNodes(
                Arrays.asList(blockedNode1, blockedNode2, blockedNode3));
        assertThat(blocklistTracker.getAllBlockedNodeIds())
                .containsExactlyInAnyOrder("node1", "node2", "node3");

        Collection<BlockedNode> removedNodes = blocklistTracker.removeTimeoutNodes(2L);
        assertThat(removedNodes).containsExactlyInAnyOrder(blockedNode1, blockedNode2);
        assertThat(blocklistTracker.getAllBlockedNodeIds()).containsExactlyInAnyOrder("node3");
    }
}
