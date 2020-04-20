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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.NullAwareJoinHelper;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract implementation for streaming unbounded Join operator which defines some member fields
 * can be shared between different implementations.
 */
public abstract class AbstractStreamingJoinOperator extends AbstractStreamOperator<RowData>
	implements TwoInputStreamOperator<RowData, RowData, RowData> {

	private static final long serialVersionUID = -376944622236540545L;

	protected static final String LEFT_RECORDS_STATE_NAME = "left-records";
	protected static final String RIGHT_RECORDS_STATE_NAME = "right-records";

	private final GeneratedJoinCondition generatedJoinCondition;
	protected final InternalTypeInfo<RowData> leftType;
	protected final InternalTypeInfo<RowData> rightType;

	protected final JoinInputSideSpec leftInputSideSpec;
	protected final JoinInputSideSpec rightInputSideSpec;

	/**
	 * Should filter null keys.
	 */
	private final int[] nullFilterKeys;

	/**
	 * No keys need to filter null.
	 */
	private final boolean nullSafe;

	/**
	 * Filter null to all keys.
	 */
	private final boolean filterAllNulls;

	protected final long stateRetentionTime;

	protected transient JoinConditionWithNullFilters joinCondition;
	protected transient TimestampedCollector<RowData> collector;

	public AbstractStreamingJoinOperator(
			InternalTypeInfo<RowData> leftType,
			InternalTypeInfo<RowData> rightType,
			GeneratedJoinCondition generatedJoinCondition,
			JoinInputSideSpec leftInputSideSpec,
			JoinInputSideSpec rightInputSideSpec,
			boolean[] filterNullKeys,
			long stateRetentionTime) {
		this.leftType = leftType;
		this.rightType = rightType;
		this.generatedJoinCondition = generatedJoinCondition;
		this.leftInputSideSpec = leftInputSideSpec;
		this.rightInputSideSpec = rightInputSideSpec;
		this.stateRetentionTime = stateRetentionTime;
		this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNullKeys);
		this.nullSafe = nullFilterKeys.length == 0;
		this.filterAllNulls = nullFilterKeys.length == filterNullKeys.length;
	}

	@Override
	public void open() throws Exception {
		super.open();

		JoinCondition condition = generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
		condition.setRuntimeContext(getRuntimeContext());
		condition.open(new Configuration());

		this.joinCondition = new JoinConditionWithNullFilters(condition);

		this.collector = new TimestampedCollector<>(output);
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (joinCondition != null) {
			joinCondition.backingJoinCondition.close();
		}
	}

	// ----------------------------------------------------------------------------------------
	// Utility Classes
	// ----------------------------------------------------------------------------------------

	private class JoinConditionWithNullFilters extends AbstractRichFunction implements JoinCondition {

		final JoinCondition backingJoinCondition;

		private JoinConditionWithNullFilters(JoinCondition backingJoinCondition) {
			this.backingJoinCondition = backingJoinCondition;
		}

		@Override
		public boolean apply(RowData left, RowData right) {
			if (!nullSafe) { // is not null safe, return false if any null exists
				// key is always BinaryRowData
				BinaryRowData joinKey = (BinaryRowData) getCurrentKey();
				if (filterAllNulls ? joinKey.anyNull() : joinKey.anyNull(nullFilterKeys)) {
					// find null present, return false directly
					return false;
				}
			}
			// test condition
			return backingJoinCondition.apply(left, right);
		}
	}

	/**
	 * The {@link AssociatedRecords} is the records associated to the input row. It is a wrapper
	 * of {@code List<OuterRecord>} which provides two helpful methods {@link #getRecords()} and
	 * {@link #getOuterRecords()}. See the method Javadoc for more details.
	 */
	protected static final class AssociatedRecords {
		private final List<OuterRecord> records;

		private AssociatedRecords(List<OuterRecord> records) {
			checkNotNull(records);
			this.records = records;
		}

		public boolean isEmpty() {
			return records.isEmpty();
		}

		public int size() {
			return records.size();
		}

		/**
		 * Gets the iterable of records. This is usually be called when the
		 * {@link AssociatedRecords} is from inner side.
		 */
		public Iterable<RowData> getRecords() {
			return new RecordsIterable(records);
		}

		/**
		 * Gets the iterable of {@link OuterRecord} which composites record and numOfAssociations.
		 * This is usually be called when the {@link AssociatedRecords} is from outer side.
		 */
		public Iterable<OuterRecord> getOuterRecords() {
			return records;
		}

		/**
		 * Creates an {@link AssociatedRecords} which represents the records associated to the
		 * input row.
		 */
		public static AssociatedRecords of(
			RowData input,
			boolean inputIsLeft,
			JoinRecordStateView otherSideStateView,
			JoinCondition condition) throws Exception {
			List<OuterRecord> associations = new ArrayList<>();
			if (otherSideStateView instanceof OuterJoinRecordStateView) {
				OuterJoinRecordStateView outerStateView = (OuterJoinRecordStateView) otherSideStateView;
				Iterable<Tuple2<RowData, Integer>> records = outerStateView.getRecordsAndNumOfAssociations();
				for (Tuple2<RowData, Integer> record : records) {
					boolean matched = inputIsLeft ? condition.apply(input, record.f0) : condition.apply(record.f0, input);
					if (matched) {
						associations.add(new OuterRecord(record.f0, record.f1));
					}
				}
			} else {
				Iterable<RowData> records = otherSideStateView.getRecords();
				for (RowData record : records) {
					boolean matched = inputIsLeft ? condition.apply(input, record) : condition.apply(record, input);
					if (matched) {
						// use -1 as the default number of associations
						associations.add(new OuterRecord(record, -1));
					}
				}
			}
			return new AssociatedRecords(associations);
		}

	}

	/**
	 * A lazy Iterable which transform {@code List<OuterReocord>} to {@code Iterable<RowData>}.
	 */
	private static final class RecordsIterable implements IterableIterator<RowData> {
		private final List<OuterRecord> records;
		private int index = 0;

		private RecordsIterable(List<OuterRecord> records) {
			this.records = records;
		}

		@Override
		public Iterator<RowData> iterator() {
			index = 0;
			return this;
		}

		@Override
		public boolean hasNext() {
			return index < records.size();
		}

		@Override
		public RowData next() {
			RowData row = records.get(index).record;
			index++;
			return row;
		}
	}

	/**
	 * An {@link OuterRecord} is a composite of record and {@code numOfAssociations}. The
	 * {@code numOfAssociations} represents the number of associated records in the other side.
	 * It is used when the record is from outer side (e.g. left side in LEFT OUTER JOIN).
	 * When the {@code numOfAssociations} is ZERO, we need to send a null padding row.
	 * This is useful to avoid recompute the associated numbers every time.
	 *
	 * <p>When the record is from inner side (e.g. right side in LEFT OUTER JOIN), the
	 * {@code numOfAssociations} will always be {@code -1}.
	 */
	protected static final class OuterRecord {
		public final RowData record;
		public final int numOfAssociations;

		private OuterRecord(RowData record, int numOfAssociations) {
			this.record = record;
			this.numOfAssociations = numOfAssociations;
		}
	}
}
