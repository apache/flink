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
import org.apache.flink.table.runtime.operators.window.assigners.ExplicitBoundsWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.InternalTimeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.internal.JoinGeneralWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateView;
import org.apache.flink.table.runtime.operators.window.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An operator that implements the logic for join streams windowing
 * based on a {@link WindowAssigner} and {@link Trigger}.
 *
 * <p>This is a simple version of {@link WindowJoinOperator} that assumes all its input has given
 * window attributes.
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using the inputs given window attributes:
 * {@code window_start}, {@code window_end}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes if it was assigned to multiple windows.
 * For the simple version
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
 * <p>The join output type should be: (left_type, right_type).
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class WindowJoinOperatorSimple<K, W extends Window>
		extends WindowJoinOperatorBase<K, W> {

	private static final long serialVersionUID = 1L;

	private WindowJoinOperatorSimple(
			WindowAssigner<W> leftAssigner,
			WindowAssigner<W> rightAssigner,
			Trigger<W> trigger,
			TypeSerializer<W> windowSerializer,
			InternalTypeInfo<RowData> leftType,
			InternalTypeInfo<RowData> rightType,
			GeneratedJoinCondition generatedJoinCondition,
			JoinInputSideSpec leftInputSideSpec,
			JoinInputSideSpec rightInputSideSpec,
			boolean generateNullsOnLeft,
			boolean generateNullsOnRight,
			boolean[] filterNullKeys) {
		super(leftAssigner, rightAssigner, trigger, windowSerializer, 0,
				leftType, rightType, generatedJoinCondition, leftInputSideSpec,
				rightInputSideSpec, generateNullsOnLeft, generateNullsOnRight,
				WindowAttribute.NONE, WindowAttribute.NONE, filterNullKeys);
	}

	@Override
	protected void initializeProcessFunction() {
		this.windowFunction1 = new JoinGeneralWindowProcessFunction<>(
				leftAssigner,
				joinInputView1,
				allowedLateness);
		this.windowFunction2 = new JoinGeneralWindowProcessFunction<>(
				rightAssigner,
				joinInputView2,
				allowedLateness);
	}

	@Override
	protected long getRecordTimestamp(RowData inputRow, boolean isLeft) {
		// the timestamp never expects to be used.
		return -1;
	}

	// -------------------------------------------------------------------------
	//  Inner Class
	// -------------------------------------------------------------------------

	/** Builder for {@link WindowJoinOperatorSimple}. */
	public static class Builder {
		private WindowAssigner<?> leftAssigner;
		private WindowAssigner<?> rightAssigner;
		private Trigger<?> trigger;
		private GeneratedJoinCondition joinCondition;
		private InternalTypeInfo<RowData> type1;
		private InternalTypeInfo<RowData> type2;
		private JoinInputSideSpec inputSideSpec1;
		private JoinInputSideSpec inputSideSpec2;
		private Boolean generateNullsOnLeft;
		private Boolean generateNullsOnRight;
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

		public Builder windowAttributeRefs(int[] leftWindowAttrRefs, int[] rightWindowAttrRefs) {
			checkArgument(
					leftWindowAttrRefs.length == 2
							&& rightWindowAttrRefs.length == 2,
					"window attributes should be exactly window_start and window_end");
			for (int ref : leftWindowAttrRefs) {
				checkArgument(ref > 0, "Window attribute input ref is invalid: " + ref);
			}
			for (int ref : rightWindowAttrRefs) {
				checkArgument(ref > 0, "Window attribute input ref is invalid: " + ref);
			}

			this.leftAssigner = ExplicitBoundsWindowAssigner.of(
					leftWindowAttrRefs[0], leftWindowAttrRefs[1]);
			this.rightAssigner = ExplicitBoundsWindowAssigner.of(
					rightWindowAttrRefs[0], rightWindowAttrRefs[1]);
			return this;
		}

		public Builder trigger(Trigger<?> trigger) {
			this.trigger = Objects.requireNonNull(trigger);
			return this;
		}

		public Builder eventTime() {
			checkArgument(leftAssigner != null,
					"Use Builder.assigner to set up the window assigning strategies first");
			if (leftAssigner instanceof InternalTimeWindowAssigner) {
				InternalTimeWindowAssigner timeWindowAssigner = (InternalTimeWindowAssigner) leftAssigner;
				this.leftAssigner = (WindowAssigner<?>) timeWindowAssigner.withEventTime();
			}
			if (trigger == null) {
				this.trigger = EventTimeTriggers.afterEndOfWindow();
			}
			return this;
		}

		public Builder processingTime() {
			checkArgument(leftAssigner != null,
					"Use Builder.assigner to set up the window assigning strategies first");
			if (leftAssigner instanceof InternalTimeWindowAssigner) {
				InternalTimeWindowAssigner timeWindowAssigner = (InternalTimeWindowAssigner) leftAssigner;
				this.leftAssigner = (WindowAssigner<?>) timeWindowAssigner.withProcessingTime();
			}
			if (trigger == null) {
				this.trigger = ProcessingTimeTriggers.afterEndOfWindow();
			}
			return this;
		}

		@SuppressWarnings("unchecked")
		public <K, W extends Window> WindowJoinOperatorSimple<K, W> build() {
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
			Preconditions.checkState(this.leftAssigner != null && this.rightAssigner != null,
					"Use Builder.tumble, Builder.sliding or Builder.session or Builder.assigner to "
							+ "set up the window assigning strategies");
			Preconditions.checkState(this.trigger != null,
					"Use Builder.eventTime or Builder.processingTime or Builder.trigger "
							+ "to set up the window triggering strategy");
			return new WindowJoinOperatorSimple<>(
					(WindowAssigner<W>) this.leftAssigner,
					(WindowAssigner<W>) this.rightAssigner,
					(Trigger<W>) this.trigger,
					(TypeSerializer<W>) this.leftAssigner.getWindowSerializer(new ExecutionConfig()),
					this.type1,
					this.type2,
					this.joinCondition,
					this.inputSideSpec1,
					this.inputSideSpec2,
					this.generateNullsOnLeft,
					this.generateNullsOnRight,
					this.filterNullKeys);
		}
	}

	// -------------------------------------------------------------------------
	//  Utilities
	// -------------------------------------------------------------------------

	/** Returns the builder of {@link WindowJoinOperatorSimple}. */
	public static Builder builder() {
		return new Builder();
	}
}
