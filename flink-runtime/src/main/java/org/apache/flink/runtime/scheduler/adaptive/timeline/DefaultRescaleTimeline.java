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

import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.runtime.util.BoundedFIFOQueue;
import org.apache.flink.util.AbstractID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Default implementation of {@link RescaleTimeline}. */
public class DefaultRescaleTimeline implements RescaleTimeline {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRescaleTimeline.class);

    private final Supplier<JobInformation> jobInformationGetter;

    private final BoundedFIFOQueue<Rescale> rescaleHistory;

    private final Map<TerminalState, Rescale> latestRescales;

    private final RescalesSummary rescalesSummary;

    private RescaleIdInfo rescaleIdInfo;

    /** When there are no rescales, the field would be null. */
    @Nullable private Rescale currentRescale;

    public DefaultRescaleTimeline(
            Supplier<JobInformation> jobInformationGetter, int maxHistorySize) {
        this.jobInformationGetter = jobInformationGetter;
        this.rescaleIdInfo = new RescaleIdInfo(new AbstractID(), 0L);
        this.latestRescales = new ConcurrentHashMap<>(TerminalState.values().length);
        this.rescaleHistory = new BoundedFIFOQueue<>(maxHistorySize);
        this.rescalesSummary = new RescalesSummary(maxHistorySize);
    }

    @Nullable
    @Override
    public Rescale getLatestRescale(TerminalState terminalState) {
        return latestRescales.get(terminalState);
    }

    @Nullable
    @Override
    public JobInformation getJobInformation() {
        return jobInformationGetter.get();
    }

    @Override
    public boolean isIdling() {
        return currentRescale == null || currentRescale.isTerminated();
    }

    @Override
    public RescaleIdInfo newRescale(boolean newRescaleEpoch) {
        rollingLatestRescale();
        if (!isIdling()) {
            String hintMsg =
                    String.format("Rescale %s with unexpected terminal state.", currentRescale);
            LOG.warn(hintMsg);
            throw new IllegalStateException(hintMsg);
        }
        currentRescale = new Rescale(nextRescaleId(newRescaleEpoch));
        rescaleHistory.add(currentRescale);
        rescalesSummary.addInProgress(currentRescale);
        return currentRescale.getRescaleIdInfo();
    }

    @Override
    public boolean updateRescale(RescaleUpdater rescaleUpdater) {
        if (!isIdling() && Objects.nonNull(rescaleUpdater)) {
            rescaleUpdater.update(currentRescale);
            rollingLatestRescale();
            if (Rescale.isTerminated(currentRescale)) {
                rescalesSummary.addTerminated(currentRescale);
            }
            return true;
        } else {
            if (isIdling()) {
                LOG.warn(
                        "Current rescale {} is null or terminated, so the update action is ignored.",
                        currentRescale);
                return false;
            }
            if (rescaleUpdater == null) {
                LOG.warn(
                        "The rescale updater is null for {}, so the null update action is ignored.",
                        currentRescale);
                return false;
            }
        }
        return false;
    }

    @Nullable
    Rescale currentRescale() {
        return currentRescale;
    }

    private RescaleIdInfo nextRescaleId(boolean newRescaleEpoch) {
        if (newRescaleEpoch) {
            rescaleIdInfo = new RescaleIdInfo(new AbstractID(), 1L);
        } else {
            rescaleIdInfo =
                    new RescaleIdInfo(
                            rescaleIdInfo.getResourceRequirementsId(),
                            rescaleIdInfo.getRescaleAttemptId() + 1L);
        }
        return rescaleIdInfo;
    }

    /** Rolling the last rescale for the specified status. */
    private void rollingLatestRescale() {
        if (Rescale.isTerminated(currentRescale)) {
            latestRescales.put(currentRescale.getTerminalState(), currentRescale);
        } else {
            LOG.warn(
                    "Rescale {} is not terminated now, the rolling action is ignored.",
                    currentRescale);
        }
    }
}
