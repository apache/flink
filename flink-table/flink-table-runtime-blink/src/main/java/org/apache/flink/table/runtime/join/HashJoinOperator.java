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

package org.apache.flink.table.runtime.join;

import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.generated.GeneratedJoinCondition;
import org.apache.flink.table.generated.GeneratedProjection;
import org.apache.flink.table.generated.JoinCondition;
import org.apache.flink.table.runtime.TableStreamOperator;
import org.apache.flink.table.runtime.hashtable.BinaryHashTable;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
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
public abstract class HashJoinOperator extends TableStreamOperator<BaseRow>
		implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow> {

	private static final Logger LOG = LoggerFactory.getLogger(HashJoinOperator.class);

	private final HashJoinParameter parameter;
	private final boolean reverseJoinFunction;
	private final HashJoinType type;

	private transient BinaryHashTable table;
	transient Collector<BaseRow> collector;

	transient BaseRow buildSideNullRow;
	private transient BaseRow probeSideNullRow;
	private transient JoinedRow joinedRow;
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

		final AbstractRowSerializer buildSerializer = (AbstractRowSerializer) getOperatorConfig()
			.getTypeSerializerIn1(getUserCodeClassloader());
		final AbstractRowSerializer probeSerializer = (AbstractRowSerializer) getOperatorConfig()
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
				parameter.reservedMemorySize,
				parameter.maxMemorySize,
				parameter.perRequestMemorySize,
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

		this.buildSideNullRow = new GenericRow(buildSerializer.getArity());
		this.probeSideNullRow = new GenericRow(probeSerializer.getArity());
		this.joinedRow = new JoinedRow();
		this.buildEnd = false;

		getMetricGroup().gauge("memoryUsedSizeInBytes", table::getUsedMemoryInBytes);
		getMetricGroup().gauge("numSpillFiles", table::getNumSpillFiles);
		getMetricGroup().gauge("spillInBytes", table::getSpillInBytes);

		parameter.condFuncCode = null;
		parameter.buildProjectionCode = null;
		parameter.probeProjectionCode = null;
	}

	@Override
	public void processElement1(StreamRecord<BaseRow> element) throws Exception {
		checkState(!buildEnd, "Should not build ended.");
		this.table.putBuildRow(element.getValue());
	}

	@Override
	public void processElement2(StreamRecord<BaseRow> element) throws Exception {
		checkState(buildEnd, "Should build ended.");
		if (this.table.tryProbe(element.getValue())) {
			joinWithNextKey();
		}
	}

	public void endInput1() throws Exception {
		checkState(!buildEnd, "Should not build ended.");
		LOG.info("Finish build phase.");
		buildEnd = true;
		this.table.endBuild();
	}

	public void endInput2() throws Exception {
		checkState(buildEnd, "Should build ended.");
		LOG.info("Finish probe phase.");
		while (this.table.nextMatching()) {
			joinWithNextKey();
		}
		LOG.info("Finish rebuild phase.");
	}

	private void joinWithNextKey() throws Exception {
		// we have a next record, get the iterators to the probe and build side values
		join(table.getBuildSideIterator(), table.getCurrentProbeRow());
	}

	public abstract void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception;

	void innerJoin(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
		collect(buildIter.getRow(), probeRow);
		while (buildIter.advanceNext()) {
			collect(buildIter.getRow(), probeRow);
		}
	}

	void buildOuterJoin(RowIterator<BinaryRow> buildIter) throws Exception {
		collect(buildIter.getRow(), probeSideNullRow);
		while (buildIter.advanceNext()) {
			collect(buildIter.getRow(), probeSideNullRow);
		}
	}

	void collect(BaseRow row1, BaseRow row2) throws Exception {
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
			long minMemorySize,
			long maxMemorySize,
			long eachRequestMemorySize,
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
		HashJoinParameter parameter = new HashJoinParameter(minMemorySize, maxMemorySize, eachRequestMemorySize,
				type, condFuncCode, reverseJoinFunction, filterNullKeys, buildProjectionCode, probeProjectionCode,
				tryDistinctBuildRow, buildRowSize, buildRowCount, probeRowCount, keyType);
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
		long reservedMemorySize;
		long maxMemorySize;
		long perRequestMemorySize;
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
				long reservedMemorySize, long maxMemorySize, long perRequestMemorySize, HashJoinType type,
				GeneratedJoinCondition condFuncCode, boolean reverseJoinFunction,
				boolean[] filterNullKeys,
				GeneratedProjection buildProjectionCode,
				GeneratedProjection probeProjectionCode, boolean tryDistinctBuildRow,
				int buildRowSize, long buildRowCount, long probeRowCount, RowType keyType) {
			this.reservedMemorySize = reservedMemorySize;
			this.maxMemorySize = maxMemorySize;
			this.perRequestMemorySize = perRequestMemorySize;
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
	 * Output assembled {@link JoinedRow} when build side row matched probe side row.
	 */
	private static class InnerHashJoinOperator extends HashJoinOperator {

		InnerHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					innerJoin(buildIter, probeRow);
				}
			}
		}
	}

	/**
	 * BuildOuter join.
	 * Output assembled {@link JoinedRow} when build side row matched probe side row.
	 * And if there is no match in the probe table, output {@link JoinedRow} assembled by
	 * build side row and nulls.
	 */
	private static class BuildOuterHashJoinOperator extends HashJoinOperator {

		BuildOuterHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
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
	 * Output assembled {@link JoinedRow} when probe side row matched build side row.
	 * And if there is no match in the build table, output {@link JoinedRow} assembled by
	 * nulls and probe side row.
	 */
	private static class ProbeOuterHashJoinOperator extends HashJoinOperator {

		ProbeOuterHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
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
	 * Output assembled {@link JoinedRow} when build side row matched probe side row.
	 * And if there is no match, output {@link JoinedRow} assembled by build side row and nulls or
	 * nulls and probe side row.
	 */
	private static class FullOuterHashJoinOperator extends HashJoinOperator {

		FullOuterHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
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
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
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
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
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
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) { //Probe phase
					// we must iterator to set probedSet.
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
