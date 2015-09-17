/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.languagebinding.api.java.common;

import java.io.IOException;
import java.util.HashMap;

import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.CrossOperator.DefaultCross;
import org.apache.flink.api.java.operators.CrossOperator.ProjectCross;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.JoinOperator.ProjectJoin;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UdfOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.languagebinding.api.java.common.OperationInfo.DatasizeHint;
import static org.apache.flink.languagebinding.api.java.common.OperationInfo.DatasizeHint.HUGE;
import static org.apache.flink.languagebinding.api.java.common.OperationInfo.DatasizeHint.NONE;
import static org.apache.flink.languagebinding.api.java.common.OperationInfo.DatasizeHint.TINY;
import org.apache.flink.languagebinding.api.java.common.OperationInfo.ProjectionEntry;
import org.apache.flink.languagebinding.api.java.common.streaming.Receiver;

/**
 * Generic class to construct a Flink plan based on external data.
 *
 * @param <INFO>
 */
public abstract class PlanBinder<INFO extends OperationInfo> {
	public static final String PLANBINDER_CONFIG_BCVAR_COUNT = "PLANBINDER_BCVAR_COUNT";
	public static final String PLANBINDER_CONFIG_BCVAR_NAME_PREFIX = "PLANBINDER_BCVAR_";

	protected static String FLINK_HDFS_PATH = "hdfs:/tmp";
	public static final String FLINK_TMP_DATA_DIR = System.getProperty("java.io.tmpdir") + "/flink_data";

	public static boolean DEBUG = false;

	protected HashMap<Integer, Object> sets = new HashMap();
	public static ExecutionEnvironment env;
	protected Receiver receiver;

	public static final int MAPPED_FILE_SIZE = 1024 * 1024 * 64;

	//====Plan==========================================================================================================
	protected void receivePlan() throws IOException {
		receiveParameters();
		receiveOperations();
	}

	//====Environment===================================================================================================
	/**
	 * This enum contains the identifiers for all supported environment parameters.
	 */
	private enum Parameters {
		DOP,
		MODE,
		RETRY,
		DEBUG
	}

	private void receiveParameters() throws IOException {
		Integer parameterCount = (Integer) receiver.getRecord(true);

		for (int x = 0; x < parameterCount; x++) {
			Tuple value = (Tuple) receiver.getRecord(true);
			switch (Parameters.valueOf(((String) value.getField(0)).toUpperCase())) {
				case DOP:
					Integer dop = (Integer) value.getField(1);
					env.setDegreeOfParallelism(dop);
					break;
				case MODE:
					FLINK_HDFS_PATH = (Boolean) value.getField(1) ? "file:/tmp/flink" : "hdfs:/tmp/flink";
					break;
				case RETRY:
					int retry = (Integer) value.getField(1);
					env.setNumberOfExecutionRetries(retry);
					break;
				case DEBUG:
					DEBUG = (Boolean) value.getField(1);
					break;
			}
		}
		if (env.getDegreeOfParallelism() < 0) {
			env.setDegreeOfParallelism(1);
		}
	}

	//====Operations====================================================================================================
	/**
	 * This enum contains the identifiers for all supported non-UDF DataSet operations.
	 */
	protected enum Operation {
		SOURCE_CSV, SOURCE_TEXT, SOURCE_VALUE, SOURCE_SEQ, SINK_CSV, SINK_TEXT, SINK_PRINT,
		PROJECTION, SORT, UNION, FIRST, DISTINCT, GROUPBY, AGGREGATE,
		REBALANCE, PARTITION_HASH,
		BROADCAST
	}

	/**
	 * This enum contains the identifiers for all supported UDF DataSet operations.
	 */
	protected enum AbstractOperation {
		COGROUP, CROSS, CROSS_H, CROSS_T, FILTER, FLATMAP, GROUPREDUCE, JOIN, JOIN_H, JOIN_T, MAP, REDUCE, MAPPARTITION,
	}

	protected void receiveOperations() throws IOException {
		Integer operationCount = (Integer) receiver.getRecord(true);
		for (int x = 0; x < operationCount; x++) {
			String identifier = (String) receiver.getRecord();
			Operation op = null;
			AbstractOperation aop = null;
			try {
				op = Operation.valueOf(identifier.toUpperCase());
			} catch (IllegalArgumentException iae) {
				try {
					aop = AbstractOperation.valueOf(identifier.toUpperCase());
				} catch (IllegalArgumentException iae2) {
					throw new IllegalArgumentException("Invalid operation specified: " + identifier);
				}
			}
			if (op != null) {
				switch (op) {
					case SOURCE_CSV:
						createCsvSource(createOperationInfo(op));
						break;
					case SOURCE_TEXT:
						createTextSource(createOperationInfo(op));
						break;
					case SOURCE_VALUE:
						createValueSource(createOperationInfo(op));
						break;
					case SOURCE_SEQ:
						createSequenceSource(createOperationInfo(op));
						break;
					case SINK_CSV:
						createCsvSink(createOperationInfo(op));
						break;
					case SINK_TEXT:
						createTextSink(createOperationInfo(op));
						break;
					case SINK_PRINT:
						createPrintSink(createOperationInfo(op));
						break;
					case BROADCAST:
						createBroadcastVariable(createOperationInfo(op));
						break;
					case AGGREGATE:
						createAggregationOperation(createOperationInfo(op));
						break;
					case DISTINCT:
						createDistinctOperation(createOperationInfo(op));
						break;
					case FIRST:
						createFirstOperation(createOperationInfo(op));
						break;
					case PARTITION_HASH:
						createHashPartitionOperation(createOperationInfo(op));
						break;
					case PROJECTION:
						createProjectOperation(createOperationInfo(op));
						break;
					case REBALANCE:
						createRebalanceOperation(createOperationInfo(op));
						break;
					case GROUPBY:
						createGroupOperation(createOperationInfo(op));
						break;
					case SORT:
						createSortOperation(createOperationInfo(op));
						break;
					case UNION:
						createUnionOperation(createOperationInfo(op));
						break;
				}
			}
			if (aop != null) {
				switch (aop) {
					case COGROUP:
						createCoGroupOperation(createOperationInfo(aop));
						break;
					case CROSS:
						createCrossOperation(NONE, createOperationInfo(aop));
						break;
					case CROSS_H:
						createCrossOperation(HUGE, createOperationInfo(aop));
						break;
					case CROSS_T:
						createCrossOperation(TINY, createOperationInfo(aop));
						break;
					case FILTER:
						createFilterOperation(createOperationInfo(aop));
						break;
					case FLATMAP:
						createFlatMapOperation(createOperationInfo(aop));
						break;
					case GROUPREDUCE:
						createGroupReduceOperation(createOperationInfo(aop));
						break;
					case JOIN:
						createJoinOperation(NONE, createOperationInfo(aop));
						break;
					case JOIN_H:
						createJoinOperation(HUGE, createOperationInfo(aop));
						break;
					case JOIN_T:
						createJoinOperation(TINY, createOperationInfo(aop));
						break;
					case MAP:
						createMapOperation(createOperationInfo(aop));
						break;
					case MAPPARTITION:
						createMapPartitionOperation(createOperationInfo(aop));
						break;
					case REDUCE:
						createReduceOperation(createOperationInfo(aop));
						break;
				}
			}
		}
	}

	/**
	 * This method creates an OperationInfo object based on the operation-identifier passed.
	 *
	 * @param operationIdentifier
	 * @return
	 * @throws IOException
	 */
	protected OperationInfo createOperationInfo(Operation operationIdentifier) throws IOException {
		return new OperationInfo(receiver, operationIdentifier);
	}

	/**
	 * This method creates an OperationInfo object based on the operation-identifier passed.
	 *
	 * @param operationIdentifier
	 * @return
	 * @throws IOException
	 */
	protected abstract INFO createOperationInfo(AbstractOperation operationIdentifier) throws IOException;

	private void createCsvSource(OperationInfo info) throws IOException {
		if (!(info.types instanceof CompositeType)) {
			throw new RuntimeException("The output type of a csv source has to be a tuple or a " +
				"pojo type. The derived type is " + info);
		}

		sets.put(info.setID, env.createInput(new CsvInputFormat(new Path(info.path),
			info.lineDelimiter, info.fieldDelimiter, (CompositeType)info.types), info.types)
			.name("CsvSource"));
	}

	private void createTextSource(OperationInfo info) throws IOException {
		sets.put(info.setID, env.readTextFile(info.path).name("TextSource"));
	}

	private void createValueSource(OperationInfo info) throws IOException {
		sets.put(info.setID, env.fromElements(info.values).name("ValueSource"));
	}

	private void createSequenceSource(OperationInfo info) throws IOException {
		sets.put(info.setID, env.generateSequence(info.from, info.to).name("SequenceSource"));
	}

	private void createCsvSink(OperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.writeAsCsv(info.path, info.lineDelimiter, info.fieldDelimiter, info.writeMode).name("CsvSink");
	}

	private void createTextSink(OperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.writeAsText(info.path, info.writeMode).name("TextSink");
	}

	private void createPrintSink(OperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.output(new PrintingOutputFormat(info.toError));
	}

	private void createBroadcastVariable(OperationInfo info) throws IOException {
		UdfOperator op1 = (UdfOperator) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		op1.withBroadcastSet(op2, info.name);
		Configuration c = ((UdfOperator) op1).getParameters();

		if (c == null) {
			c = new Configuration();
		}

		int count = c.getInteger(PLANBINDER_CONFIG_BCVAR_COUNT, 0);
		c.setInteger(PLANBINDER_CONFIG_BCVAR_COUNT, count + 1);
		c.setString(PLANBINDER_CONFIG_BCVAR_NAME_PREFIX + count, info.name);

		op1.withParameters(c);
	}

	private void createAggregationOperation(OperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		AggregateOperator ao = op.aggregate(info.aggregates[0].agg, info.aggregates[0].field);

		for (int x = 1; x < info.count; x++) {
			ao = ao.and(info.aggregates[x].agg, info.aggregates[x].field);
		}

		sets.put(info.setID, ao.name("Aggregation"));
	}

	private void createDistinctOperation(OperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, info.keys.length == 0 ? op.distinct() : op.distinct(info.keys).name("Distinct"));
	}

	private void createFirstOperation(OperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.first(info.count).name("First"));
	}

	private void createGroupOperation(OperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.groupBy(info.keys));
	}

	private void createHashPartitionOperation(OperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.partitionByHash(info.keys));
	}

	private void createProjectOperation(OperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.project(info.fields).name("Projection"));
	}

	private void createRebalanceOperation(OperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.rebalance().name("Rebalance"));
	}

	private void createSortOperation(OperationInfo info) throws IOException {
		Grouping op1 = (Grouping) sets.get(info.parentID);
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, ((UnsortedGrouping) op1).sortGroup(info.field, info.order));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.setID, ((SortedGrouping) op1).sortGroup(info.field, info.order));
		}
	}

	private void createUnionOperation(OperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.setID, op1.union(op2).name("Union"));
	}

	private void createCoGroupOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.setID, applyCoGroupOperation(op1, op2, info.keys1, info.keys2, info));
	}

	private void createCrossOperation(DatasizeHint mode, INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		if (info.types != null && (info.projections == null || info.projections.length == 0)) {
			sets.put(info.setID, applyCrossOperation(op1, op2, mode, info));
		} else {
			DefaultCross defaultResult;
			switch (mode) {
				case NONE:
					defaultResult = op1.cross(op2);
					break;
				case HUGE:
					defaultResult = op1.crossWithHuge(op2);
					break;
				case TINY:
					defaultResult = op1.crossWithTiny(op2);
					break;
				default:
					throw new IllegalArgumentException("Invalid Cross mode specified: " + mode);
			}
			if (info.projections.length == 0) {
				sets.put(info.setID, defaultResult.name("DefaultCross"));
			} else {
				ProjectCross project = null;
				for (ProjectionEntry pe : info.projections) {
					switch (pe.side) {
						case FIRST:
							project = project == null ? defaultResult.projectFirst(pe.keys) : project.projectFirst(pe.keys);
							break;
						case SECOND:
							project = project == null ? defaultResult.projectSecond(pe.keys) : project.projectSecond(pe.keys);
							break;
					}
				}
				sets.put(info.setID, project.name("ProjectCross"));
			}
		}
	}

	private void createFilterOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyFilterOperation(op1, info));
	}

	private void createFlatMapOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyFlatMapOperation(op1, info));
	}

	private void createGroupReduceOperation(INFO info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.setID, applyGroupReduceOperation((DataSet) op1, info));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, applyGroupReduceOperation((UnsortedGrouping) op1, info));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.setID, applyGroupReduceOperation((SortedGrouping) op1, info));
		}
	}

	private void createJoinOperation(DatasizeHint mode, INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		if (info.types != null && (info.projections == null || info.projections.length == 0)) {
			sets.put(info.setID, applyJoinOperation(op1, op2, info.keys1, info.keys2, mode, info));
		} else {
			DefaultJoin defaultResult = createDefaultJoin(op1, op2, info.keys1, info.keys2, mode);
			if (info.projections.length == 0) {
				sets.put(info.setID, defaultResult.name("DefaultJoin"));
			} else {
				ProjectJoin project = null;
				for (ProjectionEntry pe : info.projections) {
					switch (pe.side) {
						case FIRST:
							project = project == null ? defaultResult.projectFirst(pe.keys) : project.projectFirst(pe.keys);
							break;
						case SECOND:
							project = project == null ? defaultResult.projectSecond(pe.keys) : project.projectSecond(pe.keys);
							break;
					}
				}
				sets.put(info.setID, project.name("ProjectJoin"));
			}
		}
	}

	protected DefaultJoin createDefaultJoin(DataSet op1, DataSet op2, String[] firstKeys, String[] secondKeys, DatasizeHint mode) {
		switch (mode) {
			case NONE:
				return op1.join(op2).where(firstKeys).equalTo(secondKeys);
			case HUGE:
				return op1.joinWithHuge(op2).where(firstKeys).equalTo(secondKeys);
			case TINY:
				return op1.joinWithTiny(op2).where(firstKeys).equalTo(secondKeys);
			default:
				throw new IllegalArgumentException("Invalid join mode specified.");
		}
	}

	private void createMapOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyMapOperation(op1, info));
	}

	private void createMapPartitionOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyMapPartitionOperation(op1, info));
	}

	private void createReduceOperation(INFO info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.setID, applyReduceOperation((DataSet) op1, info));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, applyReduceOperation((UnsortedGrouping) op1, info));
		}
	}

	protected abstract DataSet applyCoGroupOperation(DataSet op1, DataSet op2, String[] firstKeys, String[] secondKeys, INFO info);

	protected abstract DataSet applyCrossOperation(DataSet op1, DataSet op2, DatasizeHint mode, INFO info);

	protected abstract DataSet applyFilterOperation(DataSet op1, INFO info);

	protected abstract DataSet applyFlatMapOperation(DataSet op1, INFO info);

	protected abstract DataSet applyGroupReduceOperation(DataSet op1, INFO info);

	protected abstract DataSet applyGroupReduceOperation(UnsortedGrouping op1, INFO info);

	protected abstract DataSet applyGroupReduceOperation(SortedGrouping op1, INFO info);

	protected abstract DataSet applyJoinOperation(DataSet op1, DataSet op2, String[] firstKeys, String[] secondKeys, DatasizeHint mode, INFO info);

	protected abstract DataSet applyMapOperation(DataSet op1, INFO info);

	protected abstract DataSet applyMapPartitionOperation(DataSet op1, INFO info);

	protected abstract DataSet applyReduceOperation(DataSet op1, INFO info);

	protected abstract DataSet applyReduceOperation(UnsortedGrouping op1, INFO info);

	//====Utility=======================================================================================================
	protected static String[] normalizeKeys(Object keys) {
		if (keys instanceof Tuple) {
			Tuple tupleKeys = (Tuple) keys;
			if (tupleKeys.getArity() == 0) {
				return new String[0];
			}
			if (tupleKeys.getField(0) instanceof Integer) {
				String[] stringKeys = new String[tupleKeys.getArity()];
				for (int x = 0; x < stringKeys.length; x++) {
					stringKeys[x] = "f" + (Integer) tupleKeys.getField(x);
				}
				return stringKeys;
			}
			if (tupleKeys.getField(0) instanceof String) {
				return tupleToStringArray(tupleKeys);
			}
			throw new RuntimeException("Key argument contains field that is neither an int nor a String.");
		}
		if (keys instanceof int[]) {
			int[] intKeys = (int[]) keys;
			String[] stringKeys = new String[intKeys.length];
			for (int x = 0; x < stringKeys.length; x++) {
				stringKeys[x] = "f" + intKeys[x];
			}
			return stringKeys;
		}
		throw new RuntimeException("Key argument is neither an int[] nor a Tuple.");
	}

	protected static int[] toIntArray(Object key) {
		if (key instanceof Tuple) {
			Tuple tuple = (Tuple) key;
			int[] keys = new int[tuple.getArity()];
			for (int y = 0; y < tuple.getArity(); y++) {
				keys[y] = (Integer) tuple.getField(y);
			}
			return keys;
		}
		if (key instanceof int[]) {
			return (int[]) key;
		}
		throw new RuntimeException("Key argument is neither an int[] nor a Tuple.");
	}

	protected static String[] tupleToStringArray(Tuple tuple) {
		String[] keys = new String[tuple.getArity()];
		for (int y = 0; y < tuple.getArity(); y++) {
			keys[y] = (String) tuple.getField(y);
		}
		return keys;
	}
}
