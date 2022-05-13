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

package org.apache.flink.cep.nfa.aftermatch;

import org.apache.flink.cep.nfa.sharedbuffer.EventId;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Discards every partial match that started before emitted match ended. */
public final class SkipPastLastStrategy extends SkipRelativeToWholeMatchStrategy {

    public static final SkipPastLastStrategy INSTANCE = new SkipPastLastStrategy();

    private static final long serialVersionUID = -8450320065949093169L;

    private SkipPastLastStrategy() {}

    @Override
    protected EventId getPruningId(final Collection<Map<String, List<EventId>>> match) {
        EventId pruningId = null;
        for (Map<String, List<EventId>> resultMap : match) {
            for (List<EventId> eventList : resultMap.values()) {
                pruningId = max(pruningId, eventList.get(eventList.size() - 1));
            }
        }

        return pruningId;
    }

    @Override
    public String toString() {
        return "SkipPastLastStrategy{}";
    }
}
