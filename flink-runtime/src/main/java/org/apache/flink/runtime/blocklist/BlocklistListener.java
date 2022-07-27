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

import org.apache.flink.runtime.messages.Acknowledge;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** A listener that want to be notified when blocklist changes. */
public interface BlocklistListener {

    /**
     * Notify new blocked node records.
     *
     * @param newNodes the new blocked node records
     * @return Future acknowledge once the new nodes have successfully notified.
     */
    CompletableFuture<Acknowledge> notifyNewBlockedNodes(Collection<BlockedNode> newNodes);
}
