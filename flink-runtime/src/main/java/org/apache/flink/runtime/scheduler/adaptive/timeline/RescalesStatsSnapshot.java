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

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.runtime.util.stats.StatsSummarySnapshot;
import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RescalesStatsSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<Rescale> rescaleHistory;
    private final Map<AbstractID, Rescale> idToRescaleMap;
    private final Map<TerminalState, Rescale> latestRescales;
    private final RescalesSummarySnapshot rescalesSummarySnapshot;

    public RescalesStatsSnapshot(
            List<Rescale> rescaleHistory,
            Map<TerminalState, Rescale> latestRescales,
            RescalesSummarySnapshot rescalesSummarySnapshot) {
        this.rescaleHistory = List.copyOf(rescaleHistory);
        this.idToRescaleMap =
                rescaleHistory.stream()
                        .collect(
                                Collectors.toMap(
                                        r -> r.getRescaleIdInfo().getRescaleUuid(),
                                        Function.identity()));
        this.latestRescales = Map.copyOf(latestRescales);
        this.rescalesSummarySnapshot = rescalesSummarySnapshot;
    }

    public List<Rescale> getRescaleHistory() {
        return rescaleHistory;
    }

    public @Nullable Rescale getRescale(AbstractID rescaleId) {
        return idToRescaleMap.get(rescaleId);
    }

    @Nullable
    public Rescale getLatestRescale(TerminalState terminalState) {
        return latestRescales.get(terminalState);
    }

    public RescalesSummarySnapshot getRescalesSummarySnapshot() {
        return rescalesSummarySnapshot;
    }

    public static RescalesStatsSnapshot emptySnapshot() {
        return new RescalesStatsSnapshot(
                new ArrayList<>(),
                new HashMap<>(),
                new RescalesSummarySnapshot(
                        StatsSummarySnapshot.empty(),
                        StatsSummarySnapshot.empty(),
                        StatsSummarySnapshot.empty(),
                        StatsSummarySnapshot.empty(),
                        0,
                        0));
    }
}
