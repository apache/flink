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

package org.apache.flink.table.runtime.operators.join.snapshot;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.metrics.SimpleGauge;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/**
 * Stream operator implementing the {@code LATERAL SNAPSHOT} processing-time temporal table join.
 *
 * <p>The operator runs in two operator-wide phases that progress only forward, LOAD then JOIN: LOAD
 * bootstraps the build-side table from its changelog up to {@code loadCompletedTime}, buffering
 * probe rows meanwhile; JOIN then continuously joins probe rows against the materialized build-side
 * state. How each input is handled depends on the current phase:
 *
 * <ul>
 *   <li>Build-side (input2 / right) changes are handled the same way in both phases: they are
 *       buffered in {@code buildChangeBuffer} and applied lazily to a per-key multi-set in {@code
 *       buildTableState} on the next per-key access (build- or probe-side) once the build-side
 *       watermark has advanced past the buffer's tag, or at the flip. The buffered changelog is
 *       applied in event-time order: changes are sorted by the build-side row-time attribute
 *       ({@code buildRowtimeIndex}), and for equal row-times retractions ({@code -U}/{@code -D})
 *       are applied before accumulations ({@code +U}/{@code +I}). Buffering until the watermark
 *       passes preserves atomic update visibility across {@code -U}/{@code +U} pairs in JOIN phase.
 *   <li>Probe-side (input1 / left) records are handled differently per phase. During LOAD they are
 *       buffered in {@code probeBuffer} until the configured flip point is reached on the
 *       build-side watermark, at which point a per-key event-time timer drains the buffered probes
 *       and joins them with the materialized build-side state. During JOIN they are joined
 *       immediately with the current build-side state.
 * </ul>
 *
 * <p>Watermark forwarding rules:
 *
 * <ul>
 *   <li>Build-side watermarks are never forwarded downstream.
 *   <li>Probe-side watermarks are held back during LOAD and forwarded during JOIN phase.
 * </ul>
 *
 * <p>The flip from LOAD to JOIN phase is triggered by either:
 *
 * <ul>
 *   <li>the build-side watermark reaching {@code loadCompletedTime} (event-time gate), or
 *   <li>the {@code loadCompletedIdleTimeoutMs} processing-time timer firing without any build-side
 *       watermark advance.
 * </ul>
 *
 * <p>State TTL eviction happens during JOIN phase and is implemented with keyed processing-time
 * timers (matching the semantics of Flink's standard {@code StateTtlConfig}).
 *
 * <p>Streaming only: The operator joins data with processing-time semantics which can't be (easily)
 * done in batch. Also, the operator's phase is held in union operator state, which is incompatible
 * with finished operators. Joins against LATERAL SNAPSHOT functions should be translated to regular
 * joins. The changes on the build-side would be consolidated into a final table and then be joined
 * with the probe-side. All probe-side records are then joined against the same (and final) version
 * of the build-side input.
 */
@Internal
public class LateralSnapshotJoinOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>, Triggerable<RowData, String> {

    // -------------------------- static final definitions --------------------------

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(LateralSnapshotJoinOperator.class);

    /** Operator state names. */
    private static final String OPERATOR_PHASE_STATE_NAME = "lateral-snapshot-phase";

    /** Keyed state names. */
    @VisibleForTesting static final String BUILD_TABLE_STATE_NAME = "build-table";

    @VisibleForTesting static final String BUILD_CHANGE_BUFFER_STATE_NAME = "build-change-buffer";
    @VisibleForTesting static final String BUFFERED_AT_WM_STATE_NAME = "buffered-at-wm";
    @VisibleForTesting static final String PROBE_BUFFER_STATE_NAME = "probe-buffer";
    private static final String TTL_EXPIRY_STATE_NAME = "ttl-expiry";

    /**
     * Single timer service for two kinds of per-key timers, distinguished by namespace: {@link
     * #FLIP_NAMESPACE} (one-shot event-time timer draining a key's LOAD-buffered probes at the
     * flip) and {@link #TTL_NAMESPACE} (recurring processing-time timer for state eviction).
     */
    private static final String TIMER_SERVICE_NAME = "lateral-snapshot-timers";

    @VisibleForTesting static final String FLIP_NAMESPACE = "flip";

    @VisibleForTesting static final String TTL_NAMESPACE = "ttl";

    /**
     * Event-time timestamp at which the per-key {@code probeBuffer} flip join timer is registered.
     * Any non-{@code MIN_VALUE} watermark advance fires it.
     */
    @VisibleForTesting static final long FLIP_JOIN_TIMER_TS = 1L;

    /** Gauge: probe-side records currently buffered during LOAD. */
    @VisibleForTesting
    static final String NUM_PROBE_BUFFERED_METRIC_NAME = "numProbeSideRecordsBuffered";

    /** Counter: keys evicted from state by TTL. */
    @VisibleForTesting
    static final String NUM_STATE_TTL_EVICTIONS_METRIC_NAME = "numStateTtlEvictions";

    /** Gauge: latest build-side watermark observed. */
    @VisibleForTesting
    static final String CURRENT_BUILD_WM_METRIC_NAME = "currentBuildSideWatermark";

    /** Gauge: latest probe-side watermark observed. */
    @VisibleForTesting
    static final String CURRENT_PROBE_WM_METRIC_NAME = "currentProbeSideWatermark";

    /** Gauge: current phase ordinal, 0 = LOAD, 1 = JOIN. */
    @VisibleForTesting static final String CURRENT_PHASE_METRIC_NAME = "currentPhase";

    /** Gauge: max joined records emitted for a probe-side record. */
    @VisibleForTesting static final String MAX_JOIN_FAN_OUT_METRIC_NAME = "maxJoinFanOut";

    /** Gauge: average joined records emitted per probe-side record. */
    @VisibleForTesting static final String AVG_JOIN_FAN_OUT_METRIC_NAME = "avgJoinFanOut";

    /** Counter: probe-side records without a match. */
    @VisibleForTesting
    static final String NUM_UNMATCHED_PROBE_METRIC_NAME = "numUnmatchedProbeRecords";

    /** Counter: build-side retractions for a row not present in state. */
    @VisibleForTesting
    static final String NUM_UNMATCHED_BUILD_RETRACTIONS_METRIC_NAME =
            "numUnmatchedBuildRetractions";

    /** Two-phase state machine. */
    enum Phase {
        LOAD,
        JOIN
    }

    /** What triggered the {@link Phase#LOAD} to {@link Phase#JOIN} flip (for logging). */
    private enum FlipTrigger {
        BUILD_WATERMARK,
        IDLE_TIMEOUT
    }

    // -------------------------- constructor args --------------------------

    private final boolean isLeftOuterJoin;
    private final InternalTypeInfo<RowData> leftType;
    private final InternalTypeInfo<RowData> rightType;

    /** Field index of the build-side (right) row-time attribute. */
    private final int buildRowtimeIndex;

    private final GeneratedJoinCondition generatedJoinCondition;

    /**
     * Per-equi-key flag indicating whether rows with a NULL in that key position must be filtered
     * before the join condition runs (SQL semantics: {@code NULL = NULL} is not true).
     */
    private final boolean[] filterNullKeys;

    /**
     * Timestamp at which the build-side watermark must arrive for the operator to flip from {@code
     * LOAD} to JOIN.
     */
    private final long loadCompletedTime;

    /**
     * Processing-time idle timeout duration (millis) on build-side watermarks. When configured, the
     * operator flips to JOIN if no build-side watermark advance is seen for this duration.
     */
    @Nullable private final Long loadCompletedIdleTimeoutMs;

    /**
     * State TTL (millis) to clean up any keyed state during JOIN phase. We schedule TTL timers
     * maxStateTtlMs ahead and check on minStateTtlMs before scheduling a new timer. This avoids
     * rescheduling timers on every state access while still ensuring that keyed state is evicted
     * after at most maxStateTtlMs of key inactivity during JOIN phase. If minStateTtlMs is set to
     * 0, state TTL is disabled.
     */
    private final long minStateTtlMs;

    private final long maxStateTtlMs;

    // -------------------------- transient runtime fields --------------------------

    private transient JoinConditionWithNullFilters joinCondition;
    private transient GenericRowData nullPaddedBuild;
    private transient TimestampedCollector<RowData> collector;

    private transient InternalTimerService<String> timerService;

    private transient Phase phase;

    /**
     * Processing-time wall clock at which the operator transitioned from {@link Phase#LOAD} to
     * {@link Phase#JOIN}. {@code null} while still in LOAD. Used by the TTL handler to reschedule
     * state TTL timers that fire too early.
     */
    @Nullable private transient Long flipProcTime;

    /** Highest build-side watermark observed. */
    private transient long currentBuildSideWm;

    /** Latest probe-side watermark observed. */
    private transient long currentProbeSideWm;

    /** Non-keyed processing-time idle-flip timer. */
    @Nullable private transient ScheduledFuture<?> idleFlipTimer;

    // -------------------------- keyed state --------------------------

    /** Build-side table as multi-set: row → reference count. */
    private transient MapState<RowData, Long> buildTableState;

    /** Buffer for build-side changes during JOIN to ensure atomic updates. */
    private transient ListState<RowData> buildChangeBuffer;

    /** Build-side watermark to ensure atomic application of build changes during JOIN. */
    private transient ValueState<Long> bufferedAtWmState;

    /** Buffer for probe-side records during LOAD. */
    private transient ListState<RowData> probeBuffer;

    /** Most recently registered TTL timer deadline; used to advance TTL timer. */
    private transient ValueState<Long> ttlExpiryState;

    // -------------------------- operator state --------------------------

    private transient ListState<Phase> operatorPhaseState;

    // -------------------------- metrics --------------------------

    private transient Counter numStateTtlEvictions;
    private transient Counter numUnmatchedProbeRecords;
    private transient Counter numUnmatchedBuildRetractions;

    private transient SimpleGauge<Long> probeBufferedGauge;
    private transient SimpleGauge<Long> buildWmGauge;
    private transient SimpleGauge<Long> probeWmGauge;
    private transient SimpleGauge<Long> maxFanOutGauge;
    private transient SimpleGauge<Double> avgFanOutGauge;
    private transient Gauge<Integer> phaseGauge;

    /** Backing accumulators for the push-model gauges (in-memory, not persisted, best-effort). */
    private transient long probeBufferedCount;

    private transient long maxJoinFanOut;
    private transient long totalJoinFanOut;
    private transient long totalProbeJoins;

    public LateralSnapshotJoinOperator(
            boolean isLeftOuterJoin,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            int buildRowtimeIndex,
            GeneratedJoinCondition generatedJoinCondition,
            boolean[] filterNullKeys,
            Long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs) {
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.leftType = Preconditions.checkNotNull(leftType);
        this.rightType = Preconditions.checkNotNull(rightType);
        this.buildRowtimeIndex = buildRowtimeIndex;
        if (buildRowtimeIndex < 0) {
            throw new IllegalArgumentException("buildRowtimeIndex must be non-negative");
        }
        if (buildRowtimeIndex >= rightType.toRowType().getFieldCount()) {
            throw new IllegalArgumentException("buildRowtimeIndex out of bounds for build row");
        }
        this.generatedJoinCondition = Preconditions.checkNotNull(generatedJoinCondition);
        this.filterNullKeys = Preconditions.checkNotNull(filterNullKeys);
        this.loadCompletedTime = Preconditions.checkNotNull(loadCompletedTime);
        if (this.loadCompletedTime < 0) {
            throw new IllegalArgumentException("loadCompletedTime must be non-negative");
        }
        this.loadCompletedIdleTimeoutMs = loadCompletedIdleTimeoutMs;
        if (this.loadCompletedIdleTimeoutMs != null && this.loadCompletedIdleTimeoutMs < 0) {
            throw new IllegalArgumentException("loadCompletedIdleTimeoutMs must be non-negative");
        }
        this.minStateTtlMs = stateTtlMs == null ? 0 : stateTtlMs;
        if (this.minStateTtlMs < 0) {
            throw new IllegalArgumentException("stateTtlMs must be non-negative");
        }
        // maxStateTtlMs is 1.5x of minStateTtlMs
        this.maxStateTtlMs = this.minStateTtlMs + this.minStateTtlMs / 2;
    }

    // -------------------------- lifecycle --------------------------

    @Override
    public boolean useInterruptibleTimers(ReadableConfig config) {
        return true;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // Operator state only — keyed state and timer services are initialized in open()
        operatorPhaseState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        OPERATOR_PHASE_STATE_NAME,
                                        new EnumSerializer<>(Phase.class)));

        // any LOAD entry → LOAD; empty (fresh start) → LOAD; else JOIN
        boolean phaseStateExists = false;
        boolean anyTaskInLoad = false;
        for (Phase persistedPhase : operatorPhaseState.get()) {
            phaseStateExists = true;
            if (persistedPhase == Phase.LOAD) {
                anyTaskInLoad = true;
                break;
            }
        }
        // we start in LOAD phase if no phaseState exists (no savepoint/checkpoint) or any task was
        // still in LOAD phase (not all tasks transitioned to JOIN phase).
        phase = (!phaseStateExists || anyTaskInLoad) ? Phase.LOAD : Phase.JOIN;

        // When restored into JOIN, anchor flipProcTime on the current wall clock so the TTL
        // handler's post-flip grace window restarts from now.
        flipProcTime =
                phase == Phase.JOIN ? getProcessingTimeService().getCurrentProcessingTime() : null;

        currentBuildSideWm = Long.MIN_VALUE;
        currentProbeSideWm = Long.MIN_VALUE;

        probeBufferedCount = 0L;
        maxJoinFanOut = 0L;
        totalJoinFanOut = 0L;
        totalProbeJoins = 0L;
    }

    @Override
    public void open() throws Exception {
        super.open();

        // Setup keyed states
        buildTableState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        BUILD_TABLE_STATE_NAME, rightType, Types.LONG));
        buildChangeBuffer =
                getRuntimeContext()
                        .getListState(
                                new ListStateDescriptor<>(
                                        BUILD_CHANGE_BUFFER_STATE_NAME, rightType));
        bufferedAtWmState =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(BUFFERED_AT_WM_STATE_NAME, Types.LONG));
        probeBuffer =
                getRuntimeContext()
                        .getListState(new ListStateDescriptor<>(PROBE_BUFFER_STATE_NAME, leftType));
        ttlExpiryState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>(TTL_EXPIRY_STATE_NAME, Types.LONG));

        // Setup timerservice
        timerService = getInternalTimerService(TIMER_SERVICE_NAME, StringSerializer.INSTANCE, this);

        // Wrap the codegen'd condition with a null-key filter so SQL semantics are honored for
        // equi-keys whose values may be NULL.
        final JoinCondition rawCondition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        joinCondition = new JoinConditionWithNullFilters(rawCondition, filterNullKeys, this);
        joinCondition.setRuntimeContext(getRuntimeContext());
        joinCondition.open(DefaultOpenContext.INSTANCE);

        // Construct null-padded record for left-outer join
        nullPaddedBuild =
                isLeftOuterJoin ? new GenericRowData(rightType.toRowType().getFieldCount()) : null;

        // Create output collector
        collector = new TimestampedCollector<>(output);

        // Register metrics
        final MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        numStateTtlEvictions = metricGroup.counter(NUM_STATE_TTL_EVICTIONS_METRIC_NAME);
        numUnmatchedProbeRecords = metricGroup.counter(NUM_UNMATCHED_PROBE_METRIC_NAME);
        numUnmatchedBuildRetractions =
                metricGroup.counter(NUM_UNMATCHED_BUILD_RETRACTIONS_METRIC_NAME);

        // Best-effort in-memory tally; not restored, so it starts at 0 after a restore/rescale.
        probeBufferedGauge = new SimpleGauge<>(probeBufferedCount);
        buildWmGauge = new SimpleGauge<>(currentBuildSideWm);
        probeWmGauge = new SimpleGauge<>(currentProbeSideWm);
        maxFanOutGauge = new SimpleGauge<>(maxJoinFanOut);
        avgFanOutGauge = new SimpleGauge<>(0.0d);
        metricGroup.gauge(NUM_PROBE_BUFFERED_METRIC_NAME, probeBufferedGauge);
        metricGroup.gauge(CURRENT_BUILD_WM_METRIC_NAME, buildWmGauge);
        metricGroup.gauge(CURRENT_PROBE_WM_METRIC_NAME, probeWmGauge);
        metricGroup.gauge(MAX_JOIN_FAN_OUT_METRIC_NAME, maxFanOutGauge);
        metricGroup.gauge(AVG_JOIN_FAN_OUT_METRIC_NAME, avgFanOutGauge);
        phaseGauge = () -> phase == null ? -1 : phase.ordinal();
        metricGroup.gauge(CURRENT_PHASE_METRIC_NAME, phaseGauge);

        // Pin the build-side input idle: the operator absorbs build-side watermarks and status.
        combinedWatermark.updateStatus(1, true);

        if (phase == Phase.LOAD && loadCompletedIdleTimeoutMs != null) {
            scheduleIdleFlipTimer();
        }

        LOG.info(
                "Opened LateralSnapshotJoinOperator: phase={}, leftOuter={}, loadCompletedTime={}, "
                        + "idleTimeoutMs={}, stateTtlMs=[{}, {}], buildRowtimeIndex={}",
                phase,
                isLeftOuterJoin,
                loadCompletedTime,
                loadCompletedIdleTimeoutMs,
                minStateTtlMs,
                maxStateTtlMs,
                buildRowtimeIndex);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        operatorPhaseState.update(List.of(phase));
    }

    @Override
    public void close() throws Exception {
        if (idleFlipTimer != null) {
            idleFlipTimer.cancel(false);
            idleFlipTimer = null;
        }
        if (joinCondition != null) {
            joinCondition.close();
        }
        super.close();
    }

    // -------------------------- input row processing --------------------------

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData probe = element.getValue();
        // Apply any buffered build-side changes if the build-side watermark has advanced.
        applyBufferedChangesIfReady();
        if (phase == Phase.LOAD) {
            probeBuffer.add(probe);
            timerService.registerEventTimeTimer(FLIP_NAMESPACE, FLIP_JOIN_TIMER_TS);
            probeBufferedGauge.update(++probeBufferedCount);
        } else {
            // Drain any probes still buffered for this key (flip drain not yet reached it) before
            // the new one, preserving intra-key order.
            if (probeBuffer.get().iterator().hasNext()) {
                drainBufferedProbes();
            }
            joinProbeRow(probe);
        }
        refreshStateTtl();
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData build = element.getValue();
        // Apply any buffered build-side changes if the build-side watermark has advanced.
        Long bufferedAt = applyBufferedChangesIfReady();
        buildChangeBuffer.add(build);
        // Tag the buffer with the current build watermark so it drains once the watermark passes.
        // The tag may move backwards on restore (currentBuildSideWm resets to MIN_VALUE), which
        // re-anchors a recovered buffer to this subtask's watermark.
        if (bufferedAt == null || bufferedAt != currentBuildSideWm) {
            bufferedAtWmState.update(currentBuildSideWm);
        }
        refreshStateTtl();
    }

    // -------------------------- watermark processing --------------------------

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        // Probe-side watermark: held back during LOAD (neither advances the timer service nor is
        // forwarded), forwarded during JOIN.
        if (phase == Phase.JOIN) {
            super.processWatermark1(mark);
        }
        currentProbeSideWm = Math.max(currentProbeSideWm, mark.getTimestamp());
        probeWmGauge.update(currentProbeSideWm);
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        // Build-side watermark: NEVER forwarded; never advances the timer service.
        long ts = mark.getTimestamp();
        currentBuildSideWm = Math.max(currentBuildSideWm, ts);
        buildWmGauge.update(currentBuildSideWm);
        if (phase == Phase.LOAD) {
            if (currentBuildSideWm >= loadCompletedTime) {
                // we reached the flip point. Transition to JOIN phase.
                transitionToJoinPhase(FlipTrigger.BUILD_WATERMARK);
            } else if (loadCompletedIdleTimeoutMs != null) {
                // we got a new build-side wm. Reschedule the idle timer (if it was configured)
                rescheduleIdleFlipTimer();
            }
        }
    }

    @Override
    protected void processWatermarkStatus(WatermarkStatus watermarkStatus, int index)
            throws Exception {
        if (index == 1) {
            // Build-side idle status is absorbed; combined watermark is probe-driven.
            return;
        }
        if (phase == Phase.LOAD) {
            // LOAD emits nothing downstream; track partial[0]'s idle bit for use after the flip.
            combinedWatermark.updateStatus(0, watermarkStatus.isIdle());
            return;
        }
        super.processWatermarkStatus(watermarkStatus, index);
    }

    // -------------------------- timers processing --------------------------

    @Override
    public void onEventTime(InternalTimer<RowData, String> timer) throws Exception {
        String namespace = timer.getNamespace();
        // FLIP_NAMESPACE timers drain the probes buffered during LOAD when the operator flips to
        // JOIN.
        if (FLIP_NAMESPACE.equals(namespace)) {
            drainBufferedProbes();
        }
    }

    /**
     * Applies the current key's buffered build-side changes, then joins and clears its probe-side
     * buffered records.
     */
    private void drainBufferedProbes() throws Exception {
        // If a recovery happened before, there might be buffered build-side changes.
        // Apply them before joining the buffered probe-side records.
        applyBufferedChanges();
        long drained = 0;
        for (RowData p : probeBuffer.get()) {
            joinProbeRow(p);
            drained++;
        }
        probeBuffer.clear();
        // Best-effort count (not restored), so floor at 0.
        probeBufferedCount = Math.max(0, probeBufferedCount - drained);
        probeBufferedGauge.update(probeBufferedCount);
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, String> timer) throws Exception {
        // TTL timers run on processing time so semantics match Flink's standard StateTtlConfig.
        if (!TTL_NAMESPACE.equals(timer.getNamespace())) {
            return;
        }
        if (minStateTtlMs == 0) {
            // TTL wasn't configured and shouldn't have registered any timers.
            return;
        }
        Long deadline = ttlExpiryState.value();
        if (deadline == null || timer.getTimestamp() != deadline) {
            return; // stale timer fire
        }
        long now = getProcessingTimeService().getCurrentProcessingTime();
        // Reschedule if still in LOAD, or in JOIN but within stateTtlMs of the flip (grace window).
        if (phase == Phase.LOAD || (flipProcTime != null && now < flipProcTime + minStateTtlMs)) {
            // set the new TTL timer maxStateTtlMs ahead
            long newDeadline =
                    phase == Phase.LOAD ? now + maxStateTtlMs : flipProcTime + maxStateTtlMs;
            timerService.registerProcessingTimeTimer(TTL_NAMESPACE, newDeadline);
            ttlExpiryState.update(newDeadline);
            return;
        }
        clearAllPerKeyState();
        numStateTtlEvictions.inc();
    }

    /**
     * Registers the load-completion idle-timeout timer. No-op when the timeout is not configured.
     */
    private void scheduleIdleFlipTimer() {
        if (loadCompletedIdleTimeoutMs == null) {
            return;
        }
        long deadline =
                getProcessingTimeService().getCurrentProcessingTime() + loadCompletedIdleTimeoutMs;
        idleFlipTimer =
                getProcessingTimeService()
                        .registerTimer(
                                deadline, t -> transitionToJoinPhase(FlipTrigger.IDLE_TIMEOUT));
    }

    /** Updates the idle flip timer. */
    private void rescheduleIdleFlipTimer() {
        cancelIdleFlipTimer();
        scheduleIdleFlipTimer();
    }

    /** Deactivates the currently registered idle flip timer. */
    private void cancelIdleFlipTimer() {
        if (idleFlipTimer != null) {
            idleFlipTimer.cancel(false);
            idleFlipTimer = null;
        }
    }

    // -------------------------- core logic --------------------------

    /**
     * Transition from LOAD to JOIN. Runs in a NON-KEYED context (callers {@link #processWatermark2}
     * and {@link #idleFlipTimer}), so it must not touch keyed state directly; the per-key probe
     * drain happens in the {@link #FLIP_NAMESPACE} timers fired below.
     */
    private void transitionToJoinPhase(FlipTrigger trigger) throws Exception {
        if (phase == Phase.JOIN) {
            return;
        }
        LOG.info(
                "Flipping phase LOAD -> JOIN (trigger={}): buildWm={}, loadCompletedTime={}. "
                        + "Joining buffered probe records...",
                trigger,
                currentBuildSideWm,
                loadCompletedTime);
        phase = Phase.JOIN;
        // Anchor the TTL grace window at the flip so keys loaded before it aren't evicted early.
        flipProcTime = getProcessingTimeService().getCurrentProcessingTime();
        cancelIdleFlipTimer();

        // If we have seen a probe-side WM, fire the FLIP timers to drain the buffered probes. Call
        // processWatermark1 with the WM, so firing is interruptible and the watermark is forwarded
        // after all buffered probe-side records were joined and the buffers cleared.
        // If we haven't seen a probe-side WM yet, the joining is triggered by the next probe-side
        // WM.
        if (currentProbeSideWm >= FLIP_JOIN_TIMER_TS) {
            super.processWatermark1(new Watermark(currentProbeSideWm));
        }
        LOG.info(
                "Completed flip to JOIN phase (trigger={}): emittedProbeWm={}",
                trigger,
                currentProbeSideWm >= FLIP_JOIN_TIMER_TS ? currentProbeSideWm : "none");
    }

    /**
     * Joins a probe-side row against the current build-side table and applies the join predicate.
     * Returns a null-padded result if the row doesn't match any build-side row and this is a LEFT
     * OUTER join.
     */
    private void joinProbeRow(RowData probe) throws Exception {
        boolean matched = false;
        // Number of result rows emitted for this probe row (the join fan-out).
        long fanOut = 0;
        for (Map.Entry<RowData, Long> entry : buildTableState.entries()) {
            RowData buildRow = entry.getKey();
            long count = entry.getValue();
            if (joinCondition.apply(probe, buildRow)) {
                matched = true;
                // Each emitted record uses a fresh JoinedRowData wrapper.
                // Reusing a row object here is unsafe when subsequent collects mutate it.
                for (long i = 0; i < count; i++) {
                    JoinedRowData out = new JoinedRowData();
                    out.replace(probe, buildRow);
                    out.setRowKind(RowKind.INSERT);
                    collector.collect(out);
                    fanOut++;
                }
            }
        }
        if (!matched && isLeftOuterJoin) {
            // No join match, emit a null-padded LEFT OUTER join result
            JoinedRowData out = new JoinedRowData();
            out.replace(probe, nullPaddedBuild);
            out.setRowKind(RowKind.INSERT);
            collector.collect(out);
            fanOut++;
        }
        if (!matched && !isLeftOuterJoin) {
            numUnmatchedProbeRecords.inc();
        }
        // Update fan-out statistics (fan-out = number of result rows emitted for this probe).
        totalProbeJoins++;
        totalJoinFanOut += fanOut;
        if (fanOut > maxJoinFanOut) {
            maxJoinFanOut = fanOut;
            maxFanOutGauge.update(maxJoinFanOut);
        }
        avgFanOutGauge.update(((double) totalJoinFanOut) / totalProbeJoins);
    }

    /**
     * Applies the buffered build-side changes if the build-side watermark advanced since last
     * buffer application. This ensures that we apply buffered changes atomically once their
     * corresponding build-side WM is passed.
     *
     * @return the watermark tag of the buffer that is still in effect for this key after this call,
     *     or {@code null} if nothing is buffered.
     */
    @Nullable
    private Long applyBufferedChangesIfReady() throws Exception {
        Long bufferedAt = bufferedAtWmState.value();
        if (bufferedAt == null) {
            // Nothing buffered for this key.
            return null;
        }
        if (currentBuildSideWm > bufferedAt) {
            // the build-side wm advanced. Buffered changes can be applied atomically now.
            applyBufferedChanges();
            return null;
        } else if (phase == Phase.JOIN && currentBuildSideWm == Long.MIN_VALUE) {
            // JOIN-phase fallback: no build watermark seen by this subtask (recovery/rescale, or
            // idle-timeout flip). Apply now, since the next build watermark may never arrive.
            applyBufferedChanges();
            return null;
        }
        return bufferedAt;
    }

    /**
     * Apply all buffered build-side changes for the current key to {@code buildTableState} in
     * event-time order, then clear the buffer state.
     */
    private void applyBufferedChanges() throws Exception {
        List<RowData> changes = new ArrayList<>();
        for (RowData c : buildChangeBuffer.get()) {
            changes.add(c);
        }
        // Apply in event-time order; read the row-time before applyBuildChange mutates the kind.
        changes.sort(this::compareBuildChanges);
        for (RowData c : changes) {
            applyBuildChange(c);
        }
        clearBuildChangeBuffer();
    }

    /** Clears the per-key build-side change buffer and its watermark tag. */
    private void clearBuildChangeBuffer() {
        buildChangeBuffer.clear();
        bufferedAtWmState.clear();
    }

    /**
     * Clears all keyed state for the current key. Used by the TTL-eviction path. The {@code
     * probeBuffer} is expected to be empty in JOIN phase (where eviction runs) but is cleared
     * defensively; its best-effort gauge is intentionally not adjusted here.
     */
    private void clearAllPerKeyState() {
        buildTableState.clear();
        clearBuildChangeBuffer();
        ttlExpiryState.clear();
        probeBuffer.clear();
    }

    /**
     * Orders build-side changelog entries for deterministic, event-time-ordered application:
     * ascending by the build-side row-time attribute, then retractions ({@code -U}/{@code -D})
     * before accumulations ({@code +U}/{@code +I}) for entries sharing a row-time.
     */
    private int compareBuildChanges(RowData a, RowData b) {
        int byTime = Long.compare(a.getLong(buildRowtimeIndex), b.getLong(buildRowtimeIndex));
        if (byTime != 0) {
            return byTime;
        }
        return Integer.compare(retractionRank(a), retractionRank(b));
    }

    /** Returns {@code 0} for retracting changes ({@code -U}/{@code -D}), {@code 1} otherwise. */
    private static int retractionRank(RowData change) {
        RowKind kind = change.getRowKind();
        return (kind == RowKind.UPDATE_BEFORE || kind == RowKind.DELETE) ? 0 : 1;
    }

    /**
     * Apply a build-side change record directly to the build-table multi-set.
     *
     * <p>MUTATES the input row's {@link RowKind} to {@link RowKind#INSERT} to normalize the key
     * used for {@code buildTableState} lookups. The caller must not rely on the original kind after
     * this call returns. The mutation avoids a per-record copy on a hot path.
     */
    private void applyBuildChange(RowData change) throws Exception {
        RowKind changeType = change.getRowKind();
        change.setRowKind(RowKind.INSERT);
        Long currentCnt = buildTableState.get(change);
        if (changeType == RowKind.INSERT || changeType == RowKind.UPDATE_AFTER) {
            buildTableState.put(change, currentCnt == null ? 1L : currentCnt + 1L);
        } else {
            if (currentCnt == null || currentCnt <= 0L) {
                numUnmatchedBuildRetractions.inc();
                return;
            }
            if (currentCnt == 1L) {
                buildTableState.remove(change);
            } else {
                buildTableState.put(change, currentCnt - 1L);
            }
        }
    }

    /** If state TTL is configured, refreshes the state TTL timer if needed. */
    private void refreshStateTtl() throws Exception {
        if (minStateTtlMs == 0) {
            // Nothing to do when state TTL is not configured.
            return;
        }
        // We register it at maxStateTtlMs to avoid rearming the timer on every access.
        long now = getProcessingTimeService().getCurrentProcessingTime();
        long refreshThreshold = now + minStateTtlMs;
        Long currentTtlTimer = ttlExpiryState.value();
        if (currentTtlTimer != null && currentTtlTimer >= refreshThreshold) {
            // Existing timer still covers at least one full stateTtlMs — leave it in place.
            return;
        }
        if (currentTtlTimer != null) {
            timerService.deleteProcessingTimeTimer(TTL_NAMESPACE, currentTtlTimer);
        }
        long newDeadline = now + maxStateTtlMs;
        timerService.registerProcessingTimeTimer(TTL_NAMESPACE, newDeadline);
        ttlExpiryState.update(newDeadline);
    }

    // -------------------------- accessors (testing) --------------------------

    @VisibleForTesting
    Phase getPhase() {
        return phase;
    }

    @VisibleForTesting
    long getCurrentBuildSideWm() {
        return currentBuildSideWm;
    }

    @VisibleForTesting
    long getCurrentProbeSideWm() {
        return currentProbeSideWm;
    }

    @Nullable
    @VisibleForTesting
    Long getFlipProcTime() {
        return flipProcTime;
    }

    @VisibleForTesting
    boolean isIdleFlipTimerActive() {
        return idleFlipTimer != null;
    }

    @VisibleForTesting
    MapState<RowData, Long> getBuildTableState() {
        return buildTableState;
    }

    @VisibleForTesting
    ListState<RowData> getBuildChangeBuffer() {
        return buildChangeBuffer;
    }

    @VisibleForTesting
    ValueState<Long> getBufferedAtWmState() {
        return bufferedAtWmState;
    }

    @VisibleForTesting
    ListState<RowData> getProbeBuffer() {
        return probeBuffer;
    }

    @VisibleForTesting
    ValueState<Long> getTtlExpiryState() {
        return ttlExpiryState;
    }

    // -------------------------- accessors (metrics, testing) --------------------------

    @VisibleForTesting
    Counter getNumStateTtlEvictions() {
        return numStateTtlEvictions;
    }

    @VisibleForTesting
    Counter getNumUnmatchedProbeRecords() {
        return numUnmatchedProbeRecords;
    }

    @VisibleForTesting
    Counter getNumUnmatchedBuildRetractions() {
        return numUnmatchedBuildRetractions;
    }

    @VisibleForTesting
    SimpleGauge<Long> getNumProbeSideRecordsBufferedGauge() {
        return probeBufferedGauge;
    }

    @VisibleForTesting
    SimpleGauge<Long> getCurrentBuildSideWatermarkGauge() {
        return buildWmGauge;
    }

    @VisibleForTesting
    SimpleGauge<Long> getCurrentProbeSideWatermarkGauge() {
        return probeWmGauge;
    }

    @VisibleForTesting
    SimpleGauge<Long> getMaxJoinFanOutGauge() {
        return maxFanOutGauge;
    }

    @VisibleForTesting
    SimpleGauge<Double> getAvgJoinFanOutGauge() {
        return avgFanOutGauge;
    }

    @VisibleForTesting
    Gauge<Integer> getCurrentPhaseGauge() {
        return phaseGauge;
    }
}
