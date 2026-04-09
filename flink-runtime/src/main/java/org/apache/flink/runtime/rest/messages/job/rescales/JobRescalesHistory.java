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

package org.apache.flink.runtime.rest.messages.job.rescales;

import org.apache.flink.runtime.rest.handler.job.rescales.JobRescalesHistoryHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Response body for {@link JobRescalesHistoryHandler}. */
@Schema(name = "JobRescalesHistory")
public class JobRescalesHistory extends ArrayList<JobRescaleDetails>
        implements ResponseBody, Serializable {

    private static final long serialVersionUID = 1L;

    // a default constructor is required for collection type marshalling
    public JobRescalesHistory() {}

    public JobRescalesHistory(int initialElements) {
        super(initialElements);
    }

    public static JobRescalesHistory from(List<JobRescaleDetails> rescalesDetails) {
        JobRescalesHistory history = new JobRescalesHistory(rescalesDetails.size());
        history.addAll(rescalesDetails);
        return history;
    }

    public static JobRescalesHistory fromRescalesStatsSnapshot(RescalesStatsSnapshot snapshot) {
        return from(
                snapshot.getRescaleHistory().stream()
                        .map((Rescale rescale) -> JobRescaleDetails.fromRescale(rescale, false))
                        .collect(Collectors.toList()));
    }

    @Override
    @JsonIgnore
    public boolean isEmpty() {
        return super.isEmpty();
    }
}
