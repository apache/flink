/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.window.join;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.InternalTimeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.PanedWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.internal.JoinGeneralWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.JoinMergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.JoinPanedWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateView;
import org.apache.flink.table.runtime.operators.window.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An operator that implements the logic for join streams windowing
 * based on a {@link WindowAssigner} and {@link Trigger}.
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes if it was assigned to multiple windows by
 * the {@code WindowAssigner}.
 *
 * <p>Each input side of the join window the streams individually. They expect to
 * have the same windowing strategy, e.g. the window type and window parameters.
 * When the LHS element triggers the window firing
 * (e.g. window1 max timestamp >= current watermark),
 * that means the same window on the RHS also meet the condition and should be fired.
 * We then get the triggered window records from the views of both sides,
 * join them with the given condition and emit. Note that the inputs are buffered in the state
 * and only emitted when a window fires. The buffered data is cleaned atomically when the window
 * expires.
 *
 * <pre>
 *                    input1                 input2
 *                      |                      |
 *                |  window1 |  &lt;= join =&gt; | window2 |
 * </pre>
 *
 * <p>A window join triggers when:
 * <ul>
 *     <li>Element from LHS or RHS triggers and fires the window</li>
 *     <li>Registered event-time timers trigger</li>
 *     <li>Registered processing-time timers trigger</li>
 * </ul>
 *
 * <p>Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires,
 * the input views {@link WindowJoinRecordStateView} produces the join records
 * under the current trigger namespace to join and emit for the pane to which the {@code Trigger}
 * belongs.
 *
 * <p>The join output type should be: (left_type, right_type, window_start, window_end).
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class WindowJoinOperator<K, W extends Window>
		extends WindowJoinOperatorBase<K, W> {

	private static final long serialVersionUID = 1L;

	private final int leftRowtimeIndex;

	private final int rightRowtimeIndex;

	private WindowJoinOperator(
			WindowAssigner<W> windowAssigner,
			Trigger<W> trigger,
			TypeSerializer<W> windowSerializer,
			long allowedLateness,
			InternalTypeInfo<RowData> leftType,
			InternalTypeInfo<RowData> rightType,
			GeneratedJoinCondition generatedJoinCondition,
			JoinInputSideSpec leftInputSideSpec,
			JoinInputSideSpec rightInputSideSpec,
			boolean generateNullsOnLeft,
			boolean generateNullsOnRight,
			int leftRowtimeIndex,
			int rightRowtimeIndex,
			WindowAttribute leftWindowAttr,
			WindowAttribute rightWindowAttr,
			boolean[] filterNullKeys) {
		super(windowAssigner, windowAssigner, trigger, windowSerializer, allowedLateness,
				leftType, rightType, generatedJoinCondition, leftInputSideSpec,
				rightInputSideSpec, generateNullsOnLeft, generateNullsOnRight,
				leftWindowAttr, rightWindowAttr, filterNullKeys);
		checkArgument(!windowAssigner.isEventTime()
				|| leftRowtimeIndex >= 0 && rightRowtimeIndex >= 0);
		this.leftRowtimeIndex = leftRowtimeIndex;
		this.rightRowtimeIndex = rightRowtimeIndex;
	}

	protected void initializeProcessFunction() {
		if (leftAssigner instanceof MergingWindowAssigner) {
			this.windowFunction1 = new JoinMergingWindowProcessFunction<>(
					(MergingWindowAssigner<W>) leftAssigner,
					"window-to-state-1",
					joinInputView1,
					windowSerializer,
					allowedLateness);
			this.windowFunction2 = new JoinMergingWindowProcessFunction<>(
					(MergingWindowAssigner<W>) rightAssigner,
					"window-to-state-2",
					joinInputView2,
					windowSerializer,
					allowedLateness);
		} else if (leftAssigner instanceof PanedWindowAssigner) {
			this.windowFunction1 = new JoinPanedWindowProcessFunction<>(
					(PanedWindowAssigner<W>) leftAssigner,
					joinInputView1,
					allowedLateness);
			this.windowFunction2 = new JoinPanedWindowProcessFunction<>(
					(PanedWindowAssigner<W>) rightAssigner,
					joinInputView2,
					allowedLateness);
		} else {
			this.windowFunction1 = new JoinGeneralWindowProcessFunction<>(
					leftAssigner,
					joinInputView1,
					allowedLateness);
			this.windowFunction2 = new JoinGeneralWindowProcessFunction<>(
					rightAssigner,
					joinInputView2,
					allowedLateness);
		}
	}

	public long getRecordTimestamp(RowData inputRow, boolean isLeft) {
		int rowtimeIndex = isLeft ? leftRowtimeIndex : rightRowtimeIndex;
		return isEventTime()
				? inputRow.getLong(rowtimeIndex)
				: internalTimerService.currentProcessingTime();
	}

	// -------------------------------------------------------------------------
	//  Inner Class
	// -------------------------------------------------------------------------

	/** Builder for {@link WindowJoinOperator}. */
	public static class Builder {
		private WindowAssigner<?> windowAssigner;
		private Trigger<?> trigger;
		private GeneratedJoinCondition joinCondition;
		private InternalTypeInfo<RowData> type1;
		private InternalTypeInfo<RowData> type2;
		private JoinInputSideSpec inputSideSpec1;
		private JoinInputSideSpec inputSideSpec2;
		private Boolean generateNullsOnLeft;
		private Boolean generateNullsOnRight;
		private long allowedLateness = 0L;
		private int rowtimeIndex1 = -1;
		private int rowtimeIndex2 = -1;
		private WindowAttribute leftAttr = WindowAttribute.NONE;
		private WindowAttribute rightAttr = WindowAttribute.START_END;
		private boolean[] filterNullKeys;

		public Builder inputType(
				InternalTypeInfo<RowData> type1,
				InternalTypeInfo<RowData> type2) {
			this.type1 = Objects.requireNonNull(type1);
			this.type2 = Objects.requireNonNull(type2);
			return this;
		}

		public Builder joinType(FlinkJoinType joinType) {
			checkArgument(joinType != FlinkJoinType.ANTI && joinType != FlinkJoinType.SEMI,
					"Unsupported join type: " + joinType);
			this.generateNullsOnLeft = joinType.isRightOuter();
			this.generateNullsOnRight = joinType.isLeftOuter();
			return this;
		}

		public Builder joinCondition(GeneratedJoinCondition joinCondition) {
			this.joinCondition = Objects.requireNonNull(joinCondition);
			return this;
		}

		public Builder joinInputSpec(
				JoinInputSideSpec inputSideSpec1,
				JoinInputSideSpec inputSideSpec2) {
			this.inputSideSpec1 = Objects.requireNonNull(inputSideSpec1);
			this.inputSideSpec2 = Objects.requireNonNull(inputSideSpec2);
			return this;
		}

		/** Array of booleans to describe whether each equal join key needs to filter out nulls,
		 * thus, we can distinguish between EQUALS and IS NOT DISTINCT FROM. */
		public Builder filterNullKeys(boolean... filterNullKeys) {
			this.filterNullKeys = Objects.requireNonNull(filterNullKeys);
			return this;
		}

		public Builder assigner(WindowAssigner<?> assigner) {
			this.windowAssigner = Objects.requireNonNull(assigner);
			return this;
		}

		public Builder trigger(Trigger<?> trigger) {
			this.trigger = Objects.requireNonNull(trigger);
			return this;
		}

		public Builder tumble(Duration size) {
			checkArgument(
					windowAssigner == null,
					"Tumbling window properties already been set");
			this.windowAssigner = TumblingWindowAssigner.of(size);
			return this;
		}

		public Builder sliding(Duration size, Duration slide) {
			checkArgument(
					windowAssigner == null,
					"Sliding window properties already been set");
			this.windowAssigner = SlidingWindowAssigner.of(size, slide);
			return this;
		}

		public Builder session(Duration sessionGap) {
			checkArgument(
					windowAssigner == null,
					"Session window properties already been set");
			this.windowAssigner = SessionWindowAssigner.withGap(sessionGap);
			return this;
		}

		public Builder eventTime(int leftIndex, int rightIndex) {
			checkArgument(windowAssigner != null,
					"Use Builder.tumble, Builder.sliding or Builder.session or Builder.assigner to "
							+ "set up the window assigning strategies first");
			if (windowAssigner instanceof InternalTimeWindowAssigner) {
				InternalTimeWindowAssigner timeWindowAssigner = (InternalTimeWindowAssigner) windowAssigner;
				this.windowAssigner = (WindowAssigner<?>) timeWindowAssigner.withEventTime();
			}
			this.rowtimeIndex1 = leftIndex;
			this.rowtimeIndex2 = rightIndex;
			if (trigger == null) {
				this.trigger = EventTimeTriggers.afterEndOfWindow();
			}
			return this;
		}

		public Builder processingTime() {
			checkArgument(windowAssigner != null,
					"Use Builder.tumble, Builder.sliding or Builder.session or Builder.assigner to "
							+ "set up the window assigning strategies first");
			if (windowAssigner instanceof InternalTimeWindowAssigner) {
				InternalTimeWindowAssigner timeWindowAssigner = (InternalTimeWindowAssigner) windowAssigner;
				this.windowAssigner = (WindowAssigner<?>) timeWindowAssigner.withProcessingTime();
			}
			if (trigger == null) {
				this.trigger = ProcessingTimeTriggers.afterEndOfWindow();
			}
			return this;
		}

		public Builder allowedLateness(Duration allowedLateness) {
			checkArgument(!allowedLateness.isNegative());
			if (allowedLateness.toMillis() > 0) {
				this.allowedLateness = allowedLateness.toMillis();
			}
			return this;
		}

		public Builder windowAttribute(WindowAttribute leftAttr, WindowAttribute rightAttr) {
			this.leftAttr = Objects.requireNonNull(leftAttr);
			this.rightAttr = Objects.requireNonNull(rightAttr);
			return this;
		}

		@SuppressWarnings("unchecked")
		public <K, W extends Window> WindowJoinOperator<K, W> build() {
			Preconditions.checkState(this.type1 != null && this.type2 != null,
					"Use Builder.inputType to set up the join input data types");
			Preconditions.checkState(this.generateNullsOnLeft != null,
					"Use Builder.joinType to set up the join type");
			Preconditions.checkState(this.joinCondition != null,
					"Use Builder.joinCondition to set up the join condition");
			Preconditions.checkState(this.inputSideSpec1 != null && this.inputSideSpec2 != null,
					"Use Builder.joinInputSpec to set up the join input specifications");
			Preconditions.checkState(this.filterNullKeys != null,
					"Use Builder.filterNullKeys to set up the which join keys need to filter nulls");
			Preconditions.checkState(this.windowAssigner != null,
					"Use Builder.tumble, Builder.sliding or Builder.session or Builder.assigner to "
							+ "set up the window assigning strategies");
			Preconditions.checkState(this.trigger != null,
					"Use Builder.eventTime or Builder.processingTime or Builder.trigger "
							+ "to set up the window triggering strategy");
			return new WindowJoinOperator<>(
					(WindowAssigner<W>) this.windowAssigner,
					(Trigger<W>) this.trigger,
					(TypeSerializer<W>) this.windowAssigner.getWindowSerializer(new ExecutionConfig()),
					this.allowedLateness,
					this.type1,
					this.type2,
					this.joinCondition,
					this.inputSideSpec1,
					this.inputSideSpec2,
					this.generateNullsOnLeft,
					this.generateNullsOnRight,
					this.rowtimeIndex1,
					this.rowtimeIndex2,
					this.leftAttr,
					this.rightAttr,
					this.filterNullKeys);
		}
	}

	// -------------------------------------------------------------------------
	//  Utilities
	// -------------------------------------------------------------------------

	/** Returns the builder of {@link WindowJoinOperator}. */
	public static Builder builder() {
		return new Builder();
	}
}
