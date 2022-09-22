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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Manages partition request listener with input channel id. */
public class PartitionRequestListenerManager {
    private final Map<InputChannelID, PartitionRequestListener> listeners;

    public PartitionRequestListenerManager() {
        this.listeners = new HashMap<>();
    }

    public Collection<PartitionRequestListener> getPartitionRequestListeners() {
        return listeners.values();
    }

    public void remove(InputChannelID receiverId) {
        listeners.remove(receiverId);
    }

    public boolean isEmpty() {
        return listeners.isEmpty();
    }

    public void registerListener(PartitionRequestListener listener) {
        PartitionRequestListener previous = listeners.put(listener.getReceiverId(), listener);
        if (previous != null) {
            throw new IllegalStateException(
                    "Partition request listener with receiver "
                            + listener.getReceiverId()
                            + " has been registered.");
        }
    }

    /**
     * Remove the expire partition request listener and add it to the given timeoutListeners.
     *
     * @param now the timestamp
     * @param timeout the timeout mills
     * @param timeoutListeners the expire partition request listeners
     */
    public void removeExpiration(
            long now, long timeout, Collection<PartitionRequestListener> timeoutListeners) {
        Iterator<Map.Entry<InputChannelID, PartitionRequestListener>> iterator =
                listeners.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<InputChannelID, PartitionRequestListener> entry = iterator.next();
            PartitionRequestListener partitionRequestListener = entry.getValue();
            if ((now - partitionRequestListener.getCreateTimestamp()) > timeout) {
                timeoutListeners.add(partitionRequestListener);
                iterator.remove();
            }
        }
    }
}
