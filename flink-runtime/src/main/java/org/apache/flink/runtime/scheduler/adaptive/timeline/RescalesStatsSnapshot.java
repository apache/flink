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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RescalesStatsSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<Rescale> rescaleHistory;
    private final RescalesSummarySnapshot rescalesSummarySnapshot;

    public RescalesStatsSnapshot(
            List<Rescale> rescaleHistory, RescalesSummarySnapshot rescalesSummarySnapshot) {
        this.rescaleHistory = rescaleHistory;
        this.rescalesSummarySnapshot = rescalesSummarySnapshot;
    }

    public List<Rescale> getRescaleHistory() {
        return rescaleHistory;
    }

    public RescalesSummarySnapshot getRescalesSummarySnapshot() {
        return rescalesSummarySnapshot;
    }

    public static RescalesStatsSnapshot emptySnapshot() {
        return new RescalesStatsSnapshot(
                new ArrayList<>(),
                new RescalesSummarySnapshot(
                        StatsSummarySnapshot.empty(),
                        StatsSummarySnapshot.empty(),
                        StatsSummarySnapshot.empty(),
                        StatsSummarySnapshot.empty(),
                        0,
                        0));
    }
}
