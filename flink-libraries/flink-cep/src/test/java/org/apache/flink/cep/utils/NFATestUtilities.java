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

package org.apache.flink.cep.utils;

import org.apache.flink.cep.Event;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Base method for IT tests of {@link NFA}. It provides utility methods. */
public class NFATestUtilities {

    @Deprecated
    public static List<List<Event>> feedNFA(List<StreamRecord<Event>> inputEvents, NFA<Event> nfa)
            throws Exception {
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).build();
        return nfaTestHarness.feedRecords(inputEvents);
    }

    public static void comparePatterns(List<List<Event>> actual, List<List<Event>> expected) {
        // the order of events within a single match is not deterministic, so normalize each match
        // before comparing the matches themselves without regard to their order
        for (List<Event> p : actual) {
            p.sort(new EventComparator());
        }

        for (List<Event> p : expected) {
            p.sort(new EventComparator());
        }

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    private static class EventComparator implements Comparator<Event> {

        @Override
        public int compare(Event o1, Event o2) {
            int nameComp = o1.getName().compareTo(o2.getName());
            int priceComp = Double.compare(o1.getPrice(), o2.getPrice());
            int idComp = Integer.compare(o1.getId(), o2.getId());
            if (nameComp == 0) {
                if (priceComp == 0) {
                    return idComp;
                } else {
                    return priceComp;
                }
            } else {
                return nameComp;
            }
        }
    }

    private NFATestUtilities() {}
}
