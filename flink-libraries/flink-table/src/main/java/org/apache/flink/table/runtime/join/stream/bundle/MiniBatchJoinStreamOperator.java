/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copysecond ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join.stream.bundle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.streaming.api.bundle.CoBundleTrigger;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.codegen.CodeGenUtils;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.codegen.JoinConditionFunction;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.bundle.KeyedCoBundleOperator;
import org.apache.flink.table.runtime.join.batch.NullAwareJoinHelper;
import org.apache.flink.table.runtime.join.stream.state.CountKeySizeStateHandler;
import org.apache.flink.table.runtime.join.stream.state.EmptyJoinStateHandler;
import org.apache.flink.table.runtime.join.stream.state.JoinKeyContainPrimaryKeyStateHandler;
import org.apache.flink.table.runtime.join.stream.state.JoinKeyNotContainPrimaryKeyStateHandler;
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler;
import org.apache.flink.table.runtime.join.stream.state.WithoutPrimaryKeyStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.EmptyMatchStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.JoinKeyContainPrimaryKeyMatchStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.JoinKeyNotContainPrimaryKeyMatchStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.JoinMatchStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.OnlyEqualityConditionMatchStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.WithoutPrimaryKeyMatchStateHandler;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * MiniBatch Join base operator.
 */
@Internal
public abstract class MiniBatchJoinStreamOperator
		extends KeyedCoBundleOperator
		implements Triggerable<BaseRow, Byte> {

	private static final long serialVersionUID = 1L;

	protected final BaseRowTypeInfo leftType;
	protected final BaseRowTypeInfo rightType;

	protected GeneratedJoinConditionFunction condFuncCode;

	protected final KeySelector<BaseRow, BaseRow> leftKeySelector;
	protected final KeySelector<BaseRow, BaseRow> rightKeySelector;

	protected final BaseRowTypeInfo leftKeyType;
	protected final BaseRowTypeInfo rightKeyType;

	protected GeneratedProjection leftPkProjectCode;
	protected GeneratedProjection rightPkProjectCode;

	protected final JoinStateHandler.Type leftJoinStateType;
	protected final JoinStateHandler.Type rightJoinStateType;

	protected final long minRetentionTime;
	protected final long maxRetentionTime;
	protected final boolean stateCleaningEnabled;

	protected transient KeyedValueState<BaseRow, Long> leftTimerState;
	protected transient KeyedValueState<BaseRow, Long> rightTimerState;

	protected transient JoinConditionFunction condFunc;

	protected transient JoinStateHandler leftStateHandler;
	protected transient JoinStateHandler rightStateHandler;

	protected transient JoinedRow joinedRow;
	protected transient TimestampedCollector<BaseRow> collector;

	//the type of timer's namespace is byte, and it can make a distinction between left-side and right-side.
	protected transient InternalTimerService<Byte> internalTimerService;

	protected boolean leftIsAccRetract;
	protected boolean rightIsAccRetract;

	//Should filter null keys.
	protected boolean[] filterNullKeys;
	protected int[] nullFilterKeys;

	public MiniBatchJoinStreamOperator(
			BaseRowTypeInfo leftType,
			BaseRowTypeInfo rightType,
			GeneratedJoinConditionFunction condFuncCode,
			KeySelector<BaseRow, BaseRow> leftKeySelector,
			KeySelector<BaseRow, BaseRow> rightKeySelector,
			GeneratedProjection leftPkProjectCode,
			GeneratedProjection rightPkProjectCode,
			JoinStateHandler.Type leftJoinStateType,
			JoinStateHandler.Type rightJoinStateType,
			long maxRetentionTime,
			long minRetentionTime,
			Boolean leftIsAccRetract,
			Boolean rightIsAccRetract,
			boolean[] filterNullKeys,
			CoBundleTrigger<BaseRow, BaseRow> coBundleTrigger,
			boolean finishBundleBeforeSnapshot) {
		super(coBundleTrigger, finishBundleBeforeSnapshot);
		this.leftType = leftType;
		this.rightType = rightType;
		this.condFuncCode = condFuncCode;
		this.leftKeySelector = leftKeySelector;
		this.rightKeySelector = rightKeySelector;
		this.leftKeyType = (BaseRowTypeInfo) ((ResultTypeQueryable) leftKeySelector).getProducedType();
		this.rightKeyType = (BaseRowTypeInfo) ((ResultTypeQueryable) rightKeySelector).getProducedType();
		this.leftPkProjectCode = leftPkProjectCode;
		this.rightPkProjectCode = rightPkProjectCode;
		this.leftJoinStateType = leftJoinStateType;
		this.rightJoinStateType = rightJoinStateType;

		this.maxRetentionTime = maxRetentionTime;
		this.minRetentionTime = minRetentionTime;
		this.stateCleaningEnabled = minRetentionTime > 1;
		this.leftIsAccRetract = leftIsAccRetract;
		this.rightIsAccRetract = rightIsAccRetract;

		this.filterNullKeys = filterNullKeys;
		if (filterNullKeys == null || filterNullKeys.length == 0) {
			this.nullFilterKeys = null;
		} else {
			this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNullKeys);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		internalTimerService =
				getInternalTimerService("join-timers", ByteSerializer.INSTANCE, this);

		LOG.debug("Compiling JoinConditionFunction: {} \n\n Code:\n {}", condFuncCode.name(), condFuncCode.code());
		Class<JoinConditionFunction> condFuncClass = CodeGenUtils.compile(
				getContainingTask().getUserCodeClassLoader(), condFuncCode.name(), condFuncCode.code());
		condFuncCode = null;
		this.condFunc = condFuncClass.newInstance();

		this.collector = new TimestampedCollector<>(output);

		this.joinedRow = new JoinedRow();

		initAllStates();

		leftPkProjectCode = null;
		rightPkProjectCode = null;
	}

	private boolean isNotNullSafe() {
		return nullFilterKeys != null && nullFilterKeys.length != 0;
	}

	protected void initAllStates() throws Exception {
		this.leftStateHandler = createJoinStateHandler(leftType, leftJoinStateType, "leftJoinState",
				leftKeySelector, leftKeyType, leftPkProjectCode);

		this.rightStateHandler = createJoinStateHandler(rightType, rightJoinStateType, "rightJoinState",
				rightKeySelector, rightKeyType, rightPkProjectCode);

		if (stateCleaningEnabled) {
			this.leftTimerState = createCleanupTimeState("left-time-state");
			this.rightTimerState = createCleanupTimeState("right-time-state");
		}
	}

	protected JoinStateHandler createJoinStateHandler(BaseRowTypeInfo recordType,
			JoinStateHandler.Type type, String name, KeySelector<BaseRow, BaseRow> keySelector,
			BaseRowTypeInfo keyType, GeneratedProjection pkProjectCode) throws Exception {

		JoinStateHandler state;

		TypeSerializer<BaseRow> joinKeySer = keyType.createSerializer();

		TypeSerializer<BaseRow> recordSer = recordType.createSerializer();

		switch (type) {
			case JOIN_KEY_CONTAIN_PRIMARY_KEY:
				KeyedValueStateDescriptor<BaseRow, BaseRow> valueStateDescriptor = new KeyedValueStateDescriptor(
						name,
						joinKeySer,
						recordSer);
				state = new JoinKeyContainPrimaryKeyStateHandler(getKeyedState(valueStateDescriptor), keySelector);
				break;
			case JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY:
				Class<Projection> pkProj = CodeGenUtils.compile(
						getContainingTask().getUserCodeClassLoader(), pkProjectCode.name(), pkProjectCode.code());
				Projection<BaseRow, BaseRow> pkProjection = pkProj.newInstance();
				TypeSerializer<BaseRow> leftPkSer = (TypeSerializer<BaseRow>) DataTypes.createInternalSerializer(
						pkProjectCode.expr().resultType());

				TypeSerializer<Tuple2<BaseRow, Long>> record2TimeSer =
					new TupleTypeInfo(recordType, Types.LONG).createSerializer(new ExecutionConfig());

				KeyedMapStateDescriptor<BaseRow, BaseRow, Tuple2<BaseRow, Long>> mapStatePkDescriptor = new
						KeyedMapStateDescriptor(
						name,
						joinKeySer,
						leftPkSer,
						record2TimeSer);
				state = new JoinKeyNotContainPrimaryKeyStateHandler(getKeyedState(mapStatePkDescriptor), keySelector,
						pkProjection);
				break;
			case WITHOUT_PRIMARY_KEY:
				TypeSerializer<Tuple2<Long, Long>> count2TimeSer =
					new TupleTypeInfo(Types.LONG, Types.LONG).createSerializer(new ExecutionConfig());
				KeyedMapStateDescriptor<BaseRow, BaseRow, Tuple2<Long, Long>> mapStateCountDescriptor = new
						KeyedMapStateDescriptor(
						name,
						joinKeySer,
						recordSer,
						count2TimeSer);
				state = new WithoutPrimaryKeyStateHandler(getKeyedState(mapStateCountDescriptor), keySelector);
				break;
			case EMPTY:
				state = new EmptyJoinStateHandler();
				break;
			case COUNT_KEY_SIZE:
				KeyedValueStateDescriptor<BaseRow, Long> countKeySizeStateDescriptor = new
						KeyedValueStateDescriptor(
						name,
						joinKeySer,
						Types.LONG.createSerializer(new ExecutionConfig()));
				state = new CountKeySizeStateHandler(getKeyedState(countKeySizeStateDescriptor), keySelector);
				break;
			default:
				throw new IOException("Unrecognized type: " + type);
		}
		return state;
	}

	protected JoinMatchStateHandler createMatchStateHandler(BaseRowTypeInfo recordType,
			JoinMatchStateHandler.Type type, BaseRowTypeInfo keyType, String name,
			GeneratedProjection pkProjectCode) throws Exception {

		JoinMatchStateHandler state;

		TypeSerializer<BaseRow> recordSer = recordType.createSerializer();
		TypeSerializer<BaseRow> joinKeySer = keyType.createSerializer();
		TypeSerializer<Long> joinCntSer = LongSerializer.INSTANCE;

		switch (type) {
			case WITHOUT_PRIMARY_KEY_MATCH:
				KeyedMapStateDescriptor<BaseRow, BaseRow, Long> mapStateDescriptor = new
						KeyedMapStateDescriptor(
						name,
						joinKeySer,
						recordSer,
						joinCntSer);
				state = new WithoutPrimaryKeyMatchStateHandler(getKeyedState(mapStateDescriptor));
				break;
			case JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY_MATCH:
				TypeSerializer<BaseRow> pkSer = (TypeSerializer<BaseRow>) DataTypes.createInternalSerializer(
						pkProjectCode.expr().resultType());
				Class<Projection> pkProj = CodeGenUtils.compile(
						getContainingTask().getUserCodeClassLoader(), pkProjectCode.name(), pkProjectCode.code());
				Projection<BaseRow, BaseRow> pkProjection = pkProj.newInstance();
				KeyedMapStateDescriptor<BaseRow, BaseRow, Long> pkStateDescriptor = new
						KeyedMapStateDescriptor(
						name,
						joinKeySer,
						pkSer,
						joinCntSer);
				state = new JoinKeyNotContainPrimaryKeyMatchStateHandler(
						getKeyedState(pkStateDescriptor), pkProjection);
				break;
			case JOIN_KEY_CONTAIN_PRIMARY_KEY_MATCH:
				KeyedValueStateDescriptor<BaseRow, Long> valueStateDescriptor = new
						KeyedValueStateDescriptor(
						name,
						joinKeySer,
						joinCntSer);
				state = new JoinKeyContainPrimaryKeyMatchStateHandler(getKeyedState(valueStateDescriptor));
				break;
			case EMPTY_MATCH:
				state = new EmptyMatchStateHandler();
				break;
			case ONLY_EQUALITY_CONDITION_EMPTY_MATCH:
				KeyedValueStateDescriptor<BaseRow, Long> nonEqualValueStateDescriptor = new
						KeyedValueStateDescriptor(
						name,
						joinKeySer,
						joinCntSer);
				state = new OnlyEqualityConditionMatchStateHandler(getKeyedState(nonEqualValueStateDescriptor));
				break;
			default:
				throw new IOException("Unrecognized type: " + type);
		}
		return state;
	}

	@Override
	public void onEventTime(InternalTimer<BaseRow, Byte> timer) throws Exception {
		throw new UnsupportedOperationException("Don't support handle event time for join operator!");
	}

	protected TimestampedCollector<BaseRow> getCollector() {
		return collector;
	}

	protected KeyedValueState<BaseRow, Long> createCleanupTimeState(String timeStateName) throws Exception {
		TypeSerializer<BaseRow> joinKeySer = leftKeyType.createSerializer();
		TypeSerializer<Long> timeSer = LongSerializer.INSTANCE;
		KeyedValueStateDescriptor<BaseRow, Long> valueStateDescriptor = new KeyedValueStateDescriptor(
				"left-" + timeStateName,
				joinKeySer,
				timeSer);
		return getKeyedState(valueStateDescriptor);
	}

	protected void registerProcessingCleanupTimer(BaseRow key, long currentTime, boolean isLeft,
			KeyedValueState<BaseRow, Long> timerState) {
		if (stateCleaningEnabled) {
			// last registered timer
			Long curCleanupTime = timerState.get(key);
			// check if a cleanup timer is registered and
			// that the current cleanup timer won't delete state we need to keep
			if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
				// we need to register a new (later) timer
				long cleanupTime = currentTime + maxRetentionTime;
				// register timer and remember clean-up time
				byte namespace = (byte) (isLeft ? 1 : 2);
				internalTimerService.registerProcessingTimeTimer(namespace, cleanupTime);
				timerState.put(key, cleanupTime);
			}
		}
	}

	protected boolean needToCleanupState(BaseRow key, long timestamp, KeyedValueState<BaseRow, Long> timerState) {
		Long cleanupTime = timerState.get(key);
		// check that the triggered timer is the last registered processing time timer.
		return null != cleanupTime && timestamp == cleanupTime;
	}

	protected boolean applyCondition(BaseRow leftRow, BaseRow rightRow, BaseRow joinKey) throws Exception {
		if (isNotNullSafe()) {
			OptionalInt result = Arrays.stream(nullFilterKeys).filter(joinKey::isNullAt).findFirst();
			if (result.isPresent()) {
				return false;
			}
		}
		return condFunc.apply(leftRow, rightRow);
	}

	public List<Tuple2<BaseRow, Long>> reduceCurrentList(Iterable<BaseRow> currentList,
			JoinStateHandler currentSideStateHandler, Boolean isAccRetract) {
		if (isAccRetract ||
			currentSideStateHandler instanceof WithoutPrimaryKeyStateHandler ||
			currentSideStateHandler instanceof CountKeySizeStateHandler) {
			return appendReduceCurrentList(currentList);
		} else {
			return upsertReduceCurrentList(currentList, currentSideStateHandler);
		}
	}

	/**
	 * Reduce current input list. The f0 of the returned tuple2 is the reduced row with header
	 * always be setted to acc, the f1 is the reduced number of this row, for example ((0,1), -2)
	 */
	public List<Tuple2<BaseRow, Long>> appendReduceCurrentList(Iterable<BaseRow> currentList) {

		List<Tuple2<BaseRow, Long>> reducedList = new LinkedList<>();
		Map<BaseRow, Tuple2<Long, Long>> reduceMap = new HashMap<>();

		for (BaseRow baseRow: currentList) {
			byte header = baseRow.getHeader();
			baseRow.setHeader(BaseRowUtil.ACCUMULATE_MSG);
			// f0 means the total row number, f1 means the merged number for add/delete
			Tuple2<Long, Long> tuple2 = reduceMap.get(baseRow);
			if (tuple2 == null) {
				tuple2 = new Tuple2<Long, Long>(0L, 0L);
			}
			tuple2.f0 += 1;
			tuple2.f1 += header == BaseRowUtil.ACCUMULATE_MSG ? 1 : -1;
			reduceMap.put(baseRow, tuple2);
		}

		Iterator<BaseRow> iterator = currentList.iterator();
		while (iterator.hasNext()) {
			BaseRow deltaRow = iterator.next();
			// get total number and reduced number
			Tuple2<Long, Long> totalAndReducedNumber = null;
			deltaRow.setHeader(BaseRowUtil.ACCUMULATE_MSG);
			totalAndReducedNumber = reduceMap.get(deltaRow);
			totalAndReducedNumber.f0--;

			if (totalAndReducedNumber.f0 != 0 || totalAndReducedNumber.f1 == 0) {
				iterator.remove();
			} else {
				reducedList.add(new Tuple2<>(deltaRow, totalAndReducedNumber.f1));
			}
		}
		return reducedList;
	}

	/**
	 * Upsert Reduce. This method is more efficient if input is update stream without retraction.
	 */
	public List<Tuple2<BaseRow, Long>> upsertReduceCurrentList(Iterable<BaseRow> currentList,
			JoinStateHandler currentSideStateHandler) {
		List<Tuple2<BaseRow, Long>> reducedList = new LinkedList<>();

		if (currentSideStateHandler instanceof JoinKeyContainPrimaryKeyStateHandler) {
			// keyedValue type, return the last baseRow
			BaseRow lastRow = null;
			for (BaseRow baseRow: currentList) {
				lastRow = baseRow;
			}
			if (lastRow != null) {
				reducedList.add(Tuple2.of(lastRow, 1L));
			}
		} else if (currentSideStateHandler instanceof JoinKeyNotContainPrimaryKeyStateHandler) {
			// keyedMap type, upsert each pk
			Map<BaseRow, BaseRow> upsertMap = new HashMap<>();
			for (BaseRow baseRow: currentList) {
				currentSideStateHandler.extractCurrentPrimaryKey(baseRow);
				BaseRow pk = currentSideStateHandler.getCurrentPrimaryKey();
				upsertMap.put(pk, baseRow);
			}
			for (BaseRow baseRow: currentList) {
				currentSideStateHandler.extractCurrentPrimaryKey(baseRow);
				BaseRow pk = currentSideStateHandler.getCurrentPrimaryKey();
				if (baseRow.equals(upsertMap.get(pk))) {
					reducedList.add(Tuple2.of(baseRow, 1L));
				}
			}
		} else {
			throw new RuntimeException("This is a bug, upsertReduceCurrentList should not be called.");
		}

		return reducedList;
	}

	public void collectResult(BaseRow row) {
		collector.collect(row);
	}

	public void collectResult(BaseRow row, long times) {
		times = times < 0 ? -times : times;
		for (long i = 0; i < times; ++i) {
			collector.collect(row);
		}
	}
}
