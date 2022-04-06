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

/**
 * Manages partition request notifier with input channel id.
 */
public class InputRequestNotifierManager {
    private final Map<InputChannelID, PartitionRequestNotifier> notifiers;

    public InputRequestNotifierManager() {
        this.notifiers = new HashMap<>();
    }

    public Collection<PartitionRequestNotifier> getPartitionRequestNotifiers() {
        return notifiers.values();
    }

    public void remove(InputChannelID receiverId) {
        notifiers.remove(receiverId);
    }

    public boolean isEmpty() {
        return notifiers.isEmpty();
    }

    public void addNotifier(PartitionRequestNotifier notifier) {
        PartitionRequestNotifier previous = notifiers.put(notifier.getReceiverId(), notifier);
        if (previous != null) {
            throw new IllegalStateException("Partition request notifier with receiver " + notifier.getReceiverId() + " has been registered.");
        }
    }

    /**
     * Remove the expire partition request notifier and add it to the given timeoutNotifiers.
     *
     * @param now the timestamp
     * @param timeout the timeout mills
     * @param timeoutNotifiers the expire partition request notifier
     */
    public void removeExpiration(long now, long timeout, Collection<PartitionRequestNotifier> timeoutNotifiers) {
        Iterator<Map.Entry<InputChannelID, PartitionRequestNotifier>> iterator = notifiers.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<InputChannelID, PartitionRequestNotifier> entry = iterator.next();
            PartitionRequestNotifier partitionRequestNotifier = entry.getValue();
            if ((now - partitionRequestNotifier.getCreateTimestamp()) > timeout) {
                timeoutNotifiers.add(partitionRequestNotifier);
                iterator.remove();
            }
        }
    }
}
