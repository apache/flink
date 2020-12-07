/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file exceBinaryRow in compliance
 * with the License.  You may oBinaryRowain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHBinaryRow WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.hashtable.BinaryHashTable;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Hash join base operator.
 *
 * <p>The join operator implements the logic of a join operator at runtime. It uses a
 * hybrid-hash-join internally to match the records with equal key. The build side of the hash
 * is the first input of the match. It support all join type in {@link HashJoinType}.
 */
public abstract class HashJoinOperator extends TableStreamOperator<RowData>
		implements TwoInputStreamOperator<RowData, RowData, RowData>,
		BoundedMultiInput, InputSelectable {

	private static final Logger LOG = LoggerFactory.getLogger(HashJoinOperator.class);

	private final HashJoinParameter parameter;
	private final boolean reverseJoinFunction;
	private final HashJoinType type;

	private transient BinaryHashTable table;
	transient Collector<RowData> collector;

	transient RowData buildSideNullRow;
	private transient RowData probeSideNullRow;
	private transient JoinedRowData joinedRow;
	private transient boolean buildEnd;
	private transient JoinCondition condition;

	HashJoinOperator(HashJoinParameter parameter) {
		this.parameter = parameter;
		this.type = parameter.type;
		this.reverseJoinFunction = parameter.reverseJoinFunction;
	}

	@Override
	public void open() throws Exception {
		super.open();

		ClassLoader cl = getContainingTask().getUserCodeClassLoader();

		final AbstractRowDataSerializer buildSerializer = (AbstractRowDataSerializer) getOperatorConfig()
			.getTypeSerializerIn1(getUserCodeClassloader());
		final AbstractRowDataSerializer probeSerializer = (AbstractRowDataSerializer) getOperatorConfig()
			.getTypeSerializerIn2(getUserCodeClassloader());

		boolean hashJoinUseBitMaps = getContainingTask().getEnvironment().getTaskConfiguration()
				.getBoolean(AlgorithmOptions.HASH_JOIN_BLOOM_FILTERS);

		int parallel = getRuntimeContext().getNumberOfParallelSubtasks();

		this.condition = parameter.condFuncCode.newInstance(cl);
		condition.setRuntimeContext(getRuntimeContext());
		condition.open(new Configuration());

		this.table = new BinaryHashTable(
				getContainingTask().getJobConfiguration(),
				getContainingTask(),
				buildSerializer, probeSerializer,
				parameter.buildProjectionCode.newInstance(cl),
				parameter.probeProjectionCode.newInstance(cl),
				getContainingTask().getEnvironment().getMemoryManager(),
				computeMemorySize(),
				getContainingTask().getEnvironment().getIOManager(),
				parameter.buildRowSize,
				parameter.buildRowCount / parallel,
				hashJoinUseBitMaps,
				type,
				condition,
				reverseJoinFunction,
				parameter.filterNullKeys,
				parameter.tryDistinctBuildRow);

		this.collector = new StreamRecordCollector<>(output);

		this.buildSideNullRow = new GenericRowData(buildSerializer.getArity());
		this.probeSideNullRow = new GenericRowData(probeSerializer.getArity());
		this.joinedRow = new JoinedRowData();
		this.buildEnd = false;

		getMetricGroup().gauge("memoryUsedSizeInBytes", table::getUsedMemoryInBytes);
		getMetricGroup().gauge("numSpillFiles", table::getNumSpillFiles);
		getMetricGroup().gauge("spillInBytes", table::getSpillInBytes);

		parameter.condFuncCode = null;
		parameter.buildProjectionCode = null;
		parameter.probeProjectionCode = null;
	}

	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {
		checkState(!buildEnd, "Should not build ended.");
		this.table.putBuildRow(element.getValue());
	}

	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {
		checkState(buildEnd, "Should build ended.");
		if (this.table.tryProbe(element.getValue())) {
			joinWithNextKey();
		}
	}

	@Override
	public InputSelection nextSelection() {
		return buildEnd ? InputSelection.SECOND : InputSelection.FIRST;
	}

	@Override
	public void endInput(int inputId) throws Exception {
		switch (inputId) {
			case 1:
				checkState(!buildEnd, "Should not build ended.");
				LOG.info("Finish build phase.");
				buildEnd = true;
				this.table.endBuild();
				break;
			case 2:
				checkState(buildEnd, "Should build ended.");
				LOG.info("Finish probe phase.");
				while (this.table.nextMatching()) {
					joinWithNextKey();
				}
				LOG.info("Finish rebuild phase.");
				break;
		}
	}

	private void joinWithNextKey() throws Exception {
		// we have a next record, get the iterators to the probe and build side values
		join(table.getBuildSideIterator(), table.getCurrentProbeRow());
	}

	public abstract void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception;

	void innerJoin(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
		collect(buildIter.getRow(), probeRow);
		while (buildIter.advanceNext()) {
			collect(buildIter.getRow(), probeRow);
		}
	}

	void buildOuterJoin(RowIterator<BinaryRowData> buildIter) throws Exception {
		collect(buildIter.getRow(), probeSideNullRow);
		while (buildIter.advanceNext()) {
			collect(buildIter.getRow(), probeSideNullRow);
		}
	}

	void collect(RowData row1, RowData row2) throws Exception {
		if (reverseJoinFunction) {
			collector.collect(joinedRow.replace(row2, row1));
		} else {
			collector.collect(joinedRow.replace(row1, row2));
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (this.table != null) {
			this.table.close();
			this.table.free();
			this.table = null;
		}
		condition.close();
	}

	public static HashJoinOperator newHashJoinOperator(
			HashJoinType type,
			GeneratedJoinCondition condFuncCode,
			boolean reverseJoinFunction,
			boolean[] filterNullKeys,
			GeneratedProjection buildProjectionCode,
			GeneratedProjection probeProjectionCode,
			boolean tryDistinctBuildRow,
			int buildRowSize,
			long buildRowCount,
			long probeRowCount,
			RowType keyType) {
		HashJoinParameter parameter = new HashJoinParameter(
				type,
				condFuncCode,
				reverseJoinFunction,
				filterNullKeys,
				buildProjectionCode,
				probeProjectionCode,
				tryDistinctBuildRow,
				buildRowSize,
				buildRowCount,
				probeRowCount,
				keyType);
		switch (type) {
			case INNER:
				return new InnerHashJoinOperator(parameter);
			case BUILD_OUTER:
				return new BuildOuterHashJoinOperator(parameter);
			case PROBE_OUTER:
				return new ProbeOuterHashJoinOperator(parameter);
			case FULL_OUTER:
				return new FullOuterHashJoinOperator(parameter);
			case SEMI:
				return new SemiHashJoinOperator(parameter);
			case ANTI:
				return new AntiHashJoinOperator(parameter);
			case BUILD_LEFT_SEMI:
			case BUILD_LEFT_ANTI:
				return new BuildLeftSemiOrAntiHashJoinOperator(parameter);
			default:
				throw new IllegalArgumentException("invalid: " + type);
		}
	}

	static class HashJoinParameter implements Serializable {
		HashJoinType type;
		GeneratedJoinCondition condFuncCode;
		boolean reverseJoinFunction;
		boolean[] filterNullKeys;
		GeneratedProjection buildProjectionCode;
		GeneratedProjection probeProjectionCode;
		boolean tryDistinctBuildRow;
		int buildRowSize;
		long buildRowCount;
		long probeRowCount;
		RowType keyType;

		HashJoinParameter(
				HashJoinType type,
				GeneratedJoinCondition condFuncCode, boolean reverseJoinFunction,
				boolean[] filterNullKeys,
				GeneratedProjection buildProjectionCode,
				GeneratedProjection probeProjectionCode, boolean tryDistinctBuildRow,
				int buildRowSize, long buildRowCount, long probeRowCount, RowType keyType) {
			this.type = type;
			this.condFuncCode = condFuncCode;
			this.reverseJoinFunction = reverseJoinFunction;
			this.filterNullKeys = filterNullKeys;
			this.buildProjectionCode = buildProjectionCode;
			this.probeProjectionCode = probeProjectionCode;
			this.tryDistinctBuildRow = tryDistinctBuildRow;
			this.buildRowSize = buildRowSize;
			this.buildRowCount = buildRowCount;
			this.probeRowCount = probeRowCount;
			this.keyType = keyType;
		}
	}

	/**
	 * Inner join.
	 * Output assembled {@link JoinedRowData} when build side row matched probe side row.
	 */
	private static class InnerHashJoinOperator extends HashJoinOperator {

		InnerHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					innerJoin(buildIter, probeRow);
				}
			}
		}
	}

	/**
	 * BuildOuter join.
	 * Output assembled {@link JoinedRowData} when build side row matched probe side row.
	 * And if there is no match in the probe table, output {@link JoinedRowData} assembled by
	 * build side row and nulls.
	 */
	private static class BuildOuterHashJoinOperator extends HashJoinOperator {

		BuildOuterHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					innerJoin(buildIter, probeRow);
				} else {
					buildOuterJoin(buildIter);
				}
			}
		}
	}

	/**
	 * ProbeOuter join.
	 * Output assembled {@link JoinedRowData} when probe side row matched build side row.
	 * And if there is no match in the build table, output {@link JoinedRowData} assembled by
	 * nulls and probe side row.
	 */
	private static class ProbeOuterHashJoinOperator extends HashJoinOperator {

		ProbeOuterHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					innerJoin(buildIter, probeRow);
				}
			} else if (probeRow != null) {
				collect(buildSideNullRow, probeRow);
			}
		}
	}

	/**
	 * BuildOuter join.
	 * Output assembled {@link JoinedRowData} when build side row matched probe side row.
	 * And if there is no match, output {@link JoinedRowData} assembled by build side row and nulls or
	 * nulls and probe side row.
	 */
	private static class FullOuterHashJoinOperator extends HashJoinOperator {

		FullOuterHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					innerJoin(buildIter, probeRow);
				} else {
					buildOuterJoin(buildIter);
				}
			} else if (probeRow != null) {
				collect(buildSideNullRow, probeRow);
			}
		}
	}

	/**
	 * Semi join.
	 * Output probe side row when probe side row matched build side row.
	 */
	private static class SemiHashJoinOperator extends HashJoinOperator {

		SemiHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
			checkNotNull(probeRow);
			if (buildIter.advanceNext()) {
				collector.collect(probeRow);
			}
		}
	}

	/**
	 * Anti join.
	 * Output probe side row when probe side row not matched build side row.
	 */
	private static class AntiHashJoinOperator extends HashJoinOperator {

		AntiHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
			checkNotNull(probeRow);
			if (!buildIter.advanceNext()) {
				collector.collect(probeRow);
			}
		}
	}

	/**
	 * BuildLeftSemiOrAnti join.
	 * BuildLeftSemiJoin: Output build side row when build side row matched probe side row.
	 * BuildLeftAntiJoin: Output build side row when build side row not matched probe side row.
	 */
	private static class BuildLeftSemiOrAntiHashJoinOperator extends HashJoinOperator {

		BuildLeftSemiOrAntiHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) { //Probe phase
					// we must iterator to set probedSet.
					//noinspection StatementWithEmptyBody
					while (buildIter.advanceNext()) {}
				} else { //End Probe phase, iterator build side elements.
					collector.collect(buildIter.getRow());
					while (buildIter.advanceNext()) {
						collector.collect(buildIter.getRow());
					}
				}
			}
		}
	}
}
