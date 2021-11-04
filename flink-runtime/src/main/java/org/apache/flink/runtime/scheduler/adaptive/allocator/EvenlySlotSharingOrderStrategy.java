/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.CircleIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** A default implementation of {@link SlotOrderFunction}. */
public class EvenlySlotSharingOrderStrategy implements SlotOrderFunction {

    @Override
    public List<? extends SlotInfo> determineOrder(Collection<? extends SlotInfo> slots) {
        final Map<TaskManagerLocation, List<SlotInfo>> taskManagerSlots =
                slots.stream().collect(Collectors.groupingBy(SlotInfo::getTaskManagerLocation));
        final Comparator<TaskManagerLocation> comparator =
                Comparator.comparingInt(taskManager -> taskManagerSlots.get(taskManager).size());

        final CircleIterator<TaskManagerLocation, SlotInfo> iterator =
                new CircleIterator<>(taskManagerSlots, comparator.reversed());
        List<SlotInfo> orders = new ArrayList<>();
        while (iterator.hasNext()) {
            orders.add(iterator.next());
        }

        return orders;
    }
}
