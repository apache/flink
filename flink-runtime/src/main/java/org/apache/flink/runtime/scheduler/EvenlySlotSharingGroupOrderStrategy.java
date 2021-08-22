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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.CircleIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** A default implementation of {@link SlotSharingGroupOrderFunction}. */
public class EvenlySlotSharingGroupOrderStrategy implements SlotSharingGroupOrderFunction {

    @Override
    public List<ExecutionSlotSharingGroup> determineOrder(
            Collection<ExecutionSlotSharingGroup> groups) {
        final Map<Set<JobVertexID>, List<ExecutionSlotSharingGroup>> slotSharingGroups =
                groups.stream()
                        .collect(Collectors.groupingBy(ExecutionSlotSharingGroup::getJobVertexIds));
        final Comparator<Set<JobVertexID>> comparator = Comparator.comparingInt(Collection::size);

        final CircleIterator<Set<JobVertexID>, ExecutionSlotSharingGroup> iterator =
                new CircleIterator<>(slotSharingGroups, comparator.reversed());
        List<ExecutionSlotSharingGroup> orders = new ArrayList<>();
        while (iterator.hasNext()) {
            orders.add(iterator.next());
        }
        return orders;
    }
}
