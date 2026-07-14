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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
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
import java.util.Iterator;
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
 *       watermark has advanced past the buffer's tag, or at the flip. The buffer is keyed by the
 *       build-side row-time attribute ({@code buildRowtimeIndex}) and applied in row-time order
 *       once the build watermark reaches it; changes sharing a row-time are applied in arrival
 *       order, which mirrors the upstream emit order (a retraction directly precedes its
 *       accumulation). Gating application on the row-time preserves atomic update visibility across
 *       {@code -U}/{@code +U} pairs in JOIN phase.
 *   <li>Probe-side (input1 / left) records are handled differently per phase. During LOAD each is
 *       buffered in {@code probeBuffer} under a synthetic, increasing timestamp with one event-time
 *       timer registered per record. At the flip these timers fire (one record each, interruptibly)
 *       and join the buffered probes against the materialized build-side state in insertion order.
 *       During JOIN records are joined immediately, unless the key's flip drain has not finished
 *       yet, in which case the record is buffered behind the pending ones to preserve order.
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
 *       watermark advance. The timer is armed on the first build-side signal (record, watermark, or
 *       idle status).
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
        implements TwoInputStreamOperator<RowData, RowData, RowData>,
                Triggerable<RowData, VoidNamespace> {

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
    private static final String PROBE_BUFFER_SEQ_STATE_NAME = "probe-buffer-seq";
    private static final String TTL_EXPIRY_STATE_NAME = "ttl-expiry";

    /**
     * Single timer service for two kinds of per-key timers, distinguished by timer type (not by
     * namespace): event-time timers drain a key's LOAD-buffered probes at the flip ({@link
     * #onEventTime}), processing-time timers evict keyed state ({@link #onProcessingTime}). Both
     * use {@link VoidNamespace} so no namespace is serialized per timer.
     */
    private static final String TIMER_SERVICE_NAME = "lateral-snapshot-timers";

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
     * operator flips to JOIN if no build-side watermark advance is seen for this duration. The
     * countdown starts on the first build-side signal (record, watermark, or idle status).
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

    private transient InternalTimerService<VoidNamespace> timerService;

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

    /**
     * True if the keyed state backend iterates map entries in serialized-key order (RocksDB/ForSt),
     * so the row-time-keyed {@link #buildChangeBuffer} is already scanned in event-time order.
     */
    private transient boolean sortedStateBackend;

    // -------------------------- keyed state --------------------------

    /**
     * Build-side table as multi-set (row → reference count). Holds the committed snapshot, i.e. all
     * build changes up to the last applied build watermark; probes join against this.
     */
    private transient MapState<RowData, Long> buildTableState;

    /**
     * Build-side changes not yet visible in {@link #buildTableState}, keyed by their build row-time
     * (multiple changes with the same row-time share a list value). Changes are applied to {@code
     * buildTableState} once the build watermark reaches their row-time.
     */
    private transient MapState<Long, List<RowData>> buildChangeBuffer;

    /** Build-side watermark to ensure atomic application of build changes. */
    private transient ValueState<Long> bufferedAtWmState;

    /**
     * Buffer for probe-side records during LOAD (and for the rare JOIN-phase record that arrives
     * while a key's buffer is still draining). Keyed by a synthetic, monotonically increasing
     * timestamp so the per-record flip timers fire in insertion order. Each buffered row has one
     * event-time timer registered on its key; the timer drains exactly that row.
     */
    private transient MapState<Long, RowData> probeBuffer;

    /**
     * Next sequence number for the current key, used to derive distinct, ordered synthetic
     * timestamps for {@link #probeBuffer}. {@code null} iff the buffer is empty, so it doubles as a
     * cheap "buffer non-empty" flag on the JOIN-phase fast path.
     */
    private transient ValueState<Long> probeBufferSeq;

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
            long loadCompletedTime,
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
        this.loadCompletedTime = loadCompletedTime;
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
                        .getMapState(
                                new MapStateDescriptor<>(
                                        BUILD_CHANGE_BUFFER_STATE_NAME,
                                        Types.LONG,
                                        Types.LIST(rightType)));
        bufferedAtWmState =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(BUFFERED_AT_WM_STATE_NAME, Types.LONG));
        probeBuffer =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        PROBE_BUFFER_STATE_NAME, Types.LONG, leftType));
        probeBufferSeq =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(
                                        PROBE_BUFFER_SEQ_STATE_NAME, Types.LONG));
        ttlExpiryState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>(TTL_EXPIRY_STATE_NAME, Types.LONG));

        // Setup timerservice. Both timer kinds use VoidNamespace; they are told apart by timer type
        // (event-time -> flip drain, processing-time -> TTL eviction), not by namespace.
        timerService =
                getInternalTimerService(TIMER_SERVICE_NAME, VoidNamespaceSerializer.INSTANCE, this);

        // RocksDB/ForSt iterate map entries in serialized-key order, so the row-time-keyed build
        // change buffer is scanned in event-time order; unordered backends (heap) need a sort.
        final String backendId = getKeyedStateBackend().getBackendTypeIdentifier();
        sortedStateBackend =
                StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME.equals(backendId)
                        || StateBackendLoader.FORST_STATE_BACKEND_NAME.equals(backendId);

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
        applyBufferedChangesUpToCurrentWm();
        // Buffer during LOAD; in JOIN, buffer too if this key's flip drain has not finished yet
        // (probeBufferSeq != null), so the new probe is joined in order after the buffered ones by
        // its own timer instead of draining the whole buffer inline. Otherwise join immediately.
        if (phase == Phase.LOAD || probeBufferSeq.value() != null) {
            bufferProbe(probe);
        } else {
            joinProbeRow(probe);
        }
        refreshStateTtl();
    }

    /**
     * Buffers a probe row for the current key and registers a per-record event-time flip timer. The
     * synthetic timestamp {@code (seq + 1) - Long.MAX_VALUE} is negative (so any non-negative
     * watermark fires it) and increases with the sequence number (so timers drain in insertion
     * order on every state backend).
     */
    private void bufferProbe(RowData probe) throws Exception {
        long seq = probeBufferSeq.value() == null ? 0L : probeBufferSeq.value();
        long ts = (seq + 1) - Long.MAX_VALUE;
        probeBuffer.put(ts, probe);
        probeBufferSeq.update(seq + 1);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, ts);
        probeBufferedGauge.update(++probeBufferedCount);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        armIdleFlipTimerIfNotArmed();
        // Apply any buffered build-side changes if the build-side watermark has advanced.
        Long bufferedAt = applyBufferedChangesUpToCurrentWm();
        // Buffer the change under its build row-time (grouping changes that share a row-time).
        RowData build = element.getValue();
        long rowtime = build.getLong(buildRowtimeIndex);
        List<RowData> atRowtime = buildChangeBuffer.get(rowtime);
        if (atRowtime == null) {
            atRowtime = new ArrayList<>();
        }
        atRowtime.add(build);
        buildChangeBuffer.put(rowtime, atRowtime);
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
            if (watermarkStatus.isIdle()) {
                armIdleFlipTimerIfNotArmed();
            }
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
    public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
        // Only event-time (flip) timers are registered: each drains exactly one buffered probe for
        // its key. All of a key's timers become due at the flip; interruptible timers fire them one
        // at a time, yielding to checkpoints between firings.
        long ts = timer.getTimestamp();
        if (ts >= 0) {
            // Buffered-probe timers are always registered at negative timestamps; a non-negative
            // timer means a broken invariant and hints to a severe issue. Terminate the job.
            throw new IllegalStateException(
                    "Unexpected event-time timer timestamp " + ts + ". " + "Terminating the job");
        }
        // Apply this key's due build-side changes before joining, so probes see the materialized
        // build snapshot. Cheaply short-circuits on repeat firings within one watermark sweep and
        // when nothing is buffered for the key.
        applyBufferedChangesUpToCurrentWm();
        RowData probe = probeBuffer.get(ts);
        if (probe == null) {
            // Already drained or evicted (e.g. by TTL) before this timer fired.
            return;
        }
        joinProbeRow(probe);
        probeBuffer.remove(ts);
        probeBufferedCount = Math.max(0, probeBufferedCount - 1);
        probeBufferedGauge.update(probeBufferedCount);
        // This is the last buffered row iff it carries the highest sequence number; clearing the
        // sequence here marks the buffer empty (cheap point lookup, no isEmpty() scan).
        Long seq = probeBufferSeq.value();
        if (seq != null && ts + Long.MAX_VALUE == seq) {
            probeBufferSeq.clear();
        }
    }

    /** Removes all probe-buffer state for the current key. */
    private void clearProbeBuffer() {
        probeBuffer.clear();
        probeBufferSeq.clear();
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
        // Only processing-time (TTL) timers are registered. Semantics match Flink's standard
        // StateTtlConfig.
        if (minStateTtlMs == 0) {
            // TTL wasn't configured and shouldn't have registered any timers.
            return;
        }
        Long deadline = ttlExpiryState.value();
        if (deadline == null || timer.getTimestamp() != deadline) {
            return; // stale timer fire
        }
        long now = getProcessingTimeService().getCurrentProcessingTime();
        // Defer eviction while the key still has pending work: during LOAD, within the post-flip
        // grace window, or while probes are still buffered (not yet drained) for this key.
        boolean withinGraceWindow = flipProcTime != null && now < flipProcTime + minStateTtlMs;
        if (phase == Phase.LOAD || withinGraceWindow || probeBufferSeq.value() != null) {
            long newDeadline;
            if (phase == Phase.LOAD) {
                newDeadline = now + maxStateTtlMs;
            } else if (withinGraceWindow) {
                // the ttl timer fired, but we only "recently" flipped into JOIN phase
                newDeadline = flipProcTime + maxStateTtlMs;
            } else {
                // the ttl timer fired, but the probe-side hasn't been drained yet.
                newDeadline = now + maxStateTtlMs;
            }
            timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, newDeadline);
            ttlExpiryState.update(newDeadline);
            return;
        }
        clearAllPerKeyState();
        numStateTtlEvictions.inc();
    }

    /**
     * Arms the idle-flip timer if an idle timeout is configured, we're still in LOAD phase and
     * haven't done it yet. {@link #idleFlipTimer} is transient, so this re-arms on the first
     * build-side signal after a restart.
     */
    private void armIdleFlipTimerIfNotArmed() {
        if (phase == Phase.LOAD && loadCompletedIdleTimeoutMs != null && idleFlipTimer == null) {
            scheduleIdleFlipTimer();
        }
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
     * drain happens in the per-record event-time timers fired below.
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

        // If we have seen a (non-negative) probe-side WM, forward it so the per-record flip timers
        // fire (their synthetic timestamps are negative) and drain the buffered probes. Firing is
        // interruptible and the watermark is forwarded after the buffered probes are joined.
        // If we haven't seen a probe-side WM yet, draining is triggered by the next probe-side WM.
        if (currentProbeSideWm >= 0) {
            super.processWatermark1(new Watermark(currentProbeSideWm));
        }
        LOG.info(
                "Completed flip to JOIN phase (trigger={}): emittedProbeWm={}",
                trigger,
                currentProbeSideWm >= 0 ? currentProbeSideWm : "none");
    }

    /**
     * Joins a probe-side row against the current build-side table and applies the join predicate.
     * Returns a null-padded result if the row doesn't match any build-side row and this is a LEFT
     * OUTER join.
     */
    private void joinProbeRow(RowData probe) throws Exception {
        boolean matched = false;
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
        // Update fan-out statistics.
        totalProbeJoins++;
        totalJoinFanOut += fanOut;
        if (fanOut > maxJoinFanOut) {
            maxJoinFanOut = fanOut;
            maxFanOutGauge.update(maxJoinFanOut);
        }
        avgFanOutGauge.update(((double) totalJoinFanOut) / totalProbeJoins);
    }

    /**
     * Applies the buffered build-side changes up to the current build-side watermark. If there are
     * no buffered changes or if the watermark didn't advance since the last application, nothing is
     * done. This ensures that we apply buffered changes atomically once their corresponding
     * build-side WM is passed.
     *
     * @return the watermark up to which changes have been applied, or {@code null} if no changes
     *     remain buffered.
     */
    @Nullable
    private Long applyBufferedChangesUpToCurrentWm() throws Exception {
        Long bufferedAt = bufferedAtWmState.value();
        if (bufferedAt == null) {
            // Nothing buffered for this key.
            return null;
        }
        if (currentBuildSideWm > bufferedAt) {
            // The build-side wm advanced: apply the changes that are now due (their row-time has
            // been reached by the build-side watermark) and keep any not-yet-due changes buffered.
            return applyBufferedChangesUpTo(currentBuildSideWm);
        } else if (phase == Phase.JOIN && currentBuildSideWm == Long.MIN_VALUE) {
            // JOIN-phase fallback: no build-side watermark seen by this subtask (recovery/rescale,
            // or idle-timeout flip). Force-apply everything now, since there is no watermark to
            // gate on and the next build watermark may not arrive for a long time.
            return applyBufferedChangesUpTo(Long.MAX_VALUE);
        }
        return bufferedAt;
    }

    /**
     * Applies the buffered build-side changes with row-time {@code <= upToWm} to {@code
     * buildTableState} in event-time order. Not-yet-due changes stay buffered and the buffer's
     * watermark tag is advanced to {@code upToWm}; a fully drained buffer is cleared instead.
     *
     * @return the watermark up to which changes have been applied, or {@code null} if no changes
     *     remain buffered.
     */
    @Nullable
    private Long applyBufferedChangesUpTo(long upToWm) throws Exception {
        boolean hasKept =
                sortedStateBackend
                        ? applyDueChangesSorted(upToWm)
                        : applyDueChangesUnsorted(upToWm);
        if (hasKept) {
            bufferedAtWmState.update(upToWm);
            return upToWm;
        }
        clearBuildChangeBuffer();
        return null;
    }

    /**
     * Applies due changes on an ordered backend (RocksDB/ForSt). Row-times are yielded ascending,
     * so due buckets are applied and dropped in place and the scan stops at the first not-yet-due
     * one; values of kept buckets are never accessed.
     *
     * @return whether any not-yet-due changes remain buffered.
     */
    private boolean applyDueChangesSorted(long upToWm) throws Exception {
        Iterator<Map.Entry<Long, List<RowData>>> it = buildChangeBuffer.iterator();
        while (it.hasNext()) {
            Map.Entry<Long, List<RowData>> entry = it.next();
            if (entry.getKey() > upToWm) {
                // All remaining row-times are also not yet due.
                return true;
            }
            applyBuildChanges(entry.getValue());
            it.remove();
        }
        return false;
    }

    /**
     * Applies due changes on an unordered backend (heap). Row-times are not yielded in order, so
     * due buckets are collected and applied sorted.
     *
     * @return whether any not-yet-due changes remain buffered.
     */
    private boolean applyDueChangesUnsorted(long upToWm) throws Exception {
        // Collect the due row-times, then apply them sorted.
        List<Long> due = new ArrayList<>();
        boolean hasKept = false;
        for (Long rowtime : buildChangeBuffer.keys()) {
            if (rowtime <= upToWm) {
                due.add(rowtime);
            } else {
                hasKept = true;
            }
        }
        due.sort(Long::compareTo);
        for (long rowtime : due) {
            applyBuildChanges(buildChangeBuffer.get(rowtime));
            buildChangeBuffer.remove(rowtime);
        }
        return hasKept;
    }

    /** Applies a list of build-side changes to the {@code buildTableState} multi-set. */
    private void applyBuildChanges(List<RowData> changes) throws Exception {
        for (RowData change : changes) {
            RowKind changeType = change.getRowKind();
            change.setRowKind(RowKind.INSERT);
            Long currentCnt = buildTableState.get(change);
            if (changeType == RowKind.INSERT || changeType == RowKind.UPDATE_AFTER) {
                buildTableState.put(change, currentCnt == null ? 1L : currentCnt + 1L);
            } else {
                if (currentCnt == null || currentCnt <= 0L) {
                    numUnmatchedBuildRetractions.inc();
                    continue;
                }
                if (currentCnt == 1L) {
                    buildTableState.remove(change);
                } else {
                    buildTableState.put(change, currentCnt - 1L);
                }
            }
        }
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
        clearProbeBuffer();
        ttlExpiryState.clear();
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
            timerService.deleteProcessingTimeTimer(VoidNamespace.INSTANCE, currentTtlTimer);
        }
        long newDeadline = now + maxStateTtlMs;
        timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, newDeadline);
        ttlExpiryState.update(newDeadline);
    }

    // -------------------------- accessors (testing) --------------------------

    @VisibleForTesting
    Phase getPhase() {
        return phase;
    }

    @VisibleForTesting
    public long getMinStateTtlMs() {
        return minStateTtlMs;
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
    MapState<Long, List<RowData>> getBuildChangeBuffer() {
        return buildChangeBuffer;
    }

    @VisibleForTesting
    boolean isSortedStateBackend() {
        return sortedStateBackend;
    }

    @VisibleForTesting
    ValueState<Long> getBufferedAtWmState() {
        return bufferedAtWmState;
    }

    @VisibleForTesting
    MapState<Long, RowData> getProbeBuffer() {
        return probeBuffer;
    }

    @VisibleForTesting
    ValueState<Long> getProbeBufferSeq() {
        return probeBufferSeq;
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
