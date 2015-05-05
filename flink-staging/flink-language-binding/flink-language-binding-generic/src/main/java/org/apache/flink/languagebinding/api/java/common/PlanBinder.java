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
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
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
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
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
	private enum Operation {
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
						createCsvSource();
						break;
					case SOURCE_TEXT:
						createTextSource();
						break;
					case SOURCE_VALUE:
						createValueSource();
						break;
					case SOURCE_SEQ:
						createSequenceSource();
						break;
					case SINK_CSV:
						createCsvSink();
						break;
					case SINK_TEXT:
						createTextSink();
						break;
					case SINK_PRINT:
						createPrintSink();
						break;
					case BROADCAST:
						createBroadcastVariable();
						break;
					case AGGREGATE:
						createAggregationOperation();
						break;
					case DISTINCT:
						createDistinctOperation();
						break;
					case FIRST:
						createFirstOperation();
						break;
					case PARTITION_HASH:
						createHashPartitionOperation();
						break;
					case PROJECTION:
						createProjectOperation();
						break;
					case REBALANCE:
						createRebalanceOperation();
						break;
					case GROUPBY:
						createGroupOperation();
						break;
					case SORT:
						createSortOperation();
						break;
					case UNION:
						createUnionOperation();
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

	private void createCsvSource() throws IOException {
		int id = (Integer) receiver.getRecord(true);
		String path = (String) receiver.getRecord();
		String fieldDelimiter = (String) receiver.getRecord();
		String lineDelimiter = (String) receiver.getRecord();
		Tuple types = (Tuple) receiver.getRecord();
		sets.put(id, env.createInput(new CsvInputFormat(new Path(path), lineDelimiter, fieldDelimiter, getForObject(types)), getForObject(types)).name("CsvSource"));
	}

	private void createTextSource() throws IOException {
		int id = (Integer) receiver.getRecord(true);
		String path = (String) receiver.getRecord();
		sets.put(id, env.readTextFile(path).name("TextSource"));
	}

	private void createValueSource() throws IOException {
		int id = (Integer) receiver.getRecord(true);
		int valueCount = (Integer) receiver.getRecord(true);
		Object[] values = new Object[valueCount];
		for (int x = 0; x < valueCount; x++) {
			values[x] = receiver.getRecord();
		}
		sets.put(id, env.fromElements(values).name("ValueSource"));
	}

	private void createSequenceSource() throws IOException {
		int id = (Integer) receiver.getRecord(true);
		long from = (Long) receiver.getRecord();
		long to = (Long) receiver.getRecord();
		sets.put(id, env.generateSequence(from, to).name("SequenceSource"));
	}

	private void createCsvSink() throws IOException {
		int parentID = (Integer) receiver.getRecord(true);
		String path = (String) receiver.getRecord();
		String fieldDelimiter = (String) receiver.getRecord();
		String lineDelimiter = (String) receiver.getRecord();
		WriteMode writeMode = ((Integer) receiver.getRecord(true)) == 1
				? WriteMode.OVERWRITE
				: WriteMode.NO_OVERWRITE;
		DataSet parent = (DataSet) sets.get(parentID);
		parent.writeAsCsv(path, lineDelimiter, fieldDelimiter, writeMode).name("CsvSink");
	}

	private void createTextSink() throws IOException {
		int parentID = (Integer) receiver.getRecord(true);
		String path = (String) receiver.getRecord();
		WriteMode writeMode = ((Integer) receiver.getRecord(true)) == 1
				? WriteMode.OVERWRITE
				: WriteMode.NO_OVERWRITE;
		DataSet parent = (DataSet) sets.get(parentID);
		parent.writeAsText(path, writeMode).name("TextSink");
	}

	private void createPrintSink() throws IOException {
		int parentID = (Integer) receiver.getRecord(true);
		DataSet parent = (DataSet) sets.get(parentID);
		boolean toError = (Boolean) receiver.getRecord();
		parent.output(new PrintingOutputFormat(toError));
	}

	private void createBroadcastVariable() throws IOException {
		int parentID = (Integer) receiver.getRecord(true);
		int otherID = (Integer) receiver.getRecord(true);
		String name = (String) receiver.getRecord();
		UdfOperator op1 = (UdfOperator) sets.get(parentID);
		DataSet op2 = (DataSet) sets.get(otherID);

		op1.withBroadcastSet(op2, name);
		Configuration c = ((UdfOperator) op1).getParameters();

		if (c == null) {
			c = new Configuration();
		}

		int count = c.getInteger(PLANBINDER_CONFIG_BCVAR_COUNT, 0);
		c.setInteger(PLANBINDER_CONFIG_BCVAR_COUNT, count + 1);
		c.setString(PLANBINDER_CONFIG_BCVAR_NAME_PREFIX + count, name);

		op1.withParameters(c);
	}

	/**
	 * This method creates an OperationInfo object based on the operation-identifier passed.
	 *
	 * @param operationIdentifier
	 * @return
	 * @throws IOException
	 */
	protected abstract INFO createOperationInfo(AbstractOperation operationIdentifier) throws IOException;

	private void createAggregationOperation() throws IOException {
		int setID = (Integer) receiver.getRecord(true);
		int parentID = (Integer) receiver.getRecord(true);
		int count = (Integer) receiver.getRecord(true);

		int encodedAgg = (Integer) receiver.getRecord(true);
		int field = (Integer) receiver.getRecord(true);

		Aggregations agg = null;
		switch (encodedAgg) {
			case 0:
				agg = Aggregations.MAX;
				break;
			case 1:
				agg = Aggregations.MIN;
				break;
			case 2:
				agg = Aggregations.SUM;
				break;
		}
		DataSet op = (DataSet) sets.get(parentID);
		AggregateOperator ao = op.aggregate(agg, field);

		for (int x = 1; x < count; x++) {
			encodedAgg = (Integer) receiver.getRecord(true);
			field = (Integer) receiver.getRecord(true);
			switch (encodedAgg) {
				case 0:
					ao = ao.andMax(field);
					break;
				case 1:
					ao = ao.andMin(field);
					break;
				case 2:
					ao = ao.andSum(field);
					break;
			}
		}

		sets.put(setID, ao.name("Aggregation"));
	}

	private void createCoGroupOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.setID, applyCoGroupOperation(op1, op2, info.keys1, info.keys2, info));
	}

	protected abstract DataSet applyCoGroupOperation(DataSet op1, DataSet op2, int[] firstKeys, int[] secondKeys, INFO info);

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

	protected abstract DataSet applyCrossOperation(DataSet op1, DataSet op2, DatasizeHint mode, INFO info);

	private void createDistinctOperation() throws IOException {
		int setID = (Integer) receiver.getRecord(true);
		int parentID = (Integer) receiver.getRecord(true);
		Object keysArrayOrTuple = receiver.getRecord(true);
		int[] keys;
		if (keysArrayOrTuple instanceof Tuple) {
			keys = tupleToIntArray((Tuple) keysArrayOrTuple);
		} else {
			keys = (int[]) keysArrayOrTuple;
		}
		DataSet op = (DataSet) sets.get(parentID);
		sets.put(setID, (keys.length == 0 ? op.distinct() : op.distinct(keys)).name("Distinct"));
	}

	private void createFilterOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyFilterOperation(op1, info));
	}

	protected abstract DataSet applyFilterOperation(DataSet op1, INFO info);

	private void createFlatMapOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyFlatMapOperation(op1, info));
	}

	protected abstract DataSet applyFlatMapOperation(DataSet op1, INFO info);

	private void createFirstOperation() throws IOException {
		int setID = (Integer) receiver.getRecord(true);
		int parentID = (Integer) receiver.getRecord(true);
		int count = (Integer) receiver.getRecord(true);
		DataSet op = (DataSet) sets.get(parentID);
		sets.put(setID, op.first(count).name("First"));
	}

	private void createGroupOperation() throws IOException {
		int setID = (Integer) receiver.getRecord(true);
		int parentID = (Integer) receiver.getRecord(true);
		Object keysArrayOrTuple = receiver.getRecord(true);
		int[] keys;
		if (keysArrayOrTuple instanceof Tuple) {
			keys = tupleToIntArray((Tuple) keysArrayOrTuple);
		} else {
			keys = (int[]) keysArrayOrTuple;
		}
		DataSet op1 = (DataSet) sets.get(parentID);
		sets.put(setID, op1.groupBy(keys));
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

	protected abstract DataSet applyGroupReduceOperation(DataSet op1, INFO info);

	protected abstract DataSet applyGroupReduceOperation(UnsortedGrouping op1, INFO info);

	protected abstract DataSet applyGroupReduceOperation(SortedGrouping op1, INFO info);

	private void createHashPartitionOperation() throws IOException {
		int setID = (Integer) receiver.getRecord(true);
		int parentID = (Integer) receiver.getRecord(true);
		Object keysArrayOrTuple = receiver.getRecord(true);
		int[] keys;
		if (keysArrayOrTuple instanceof Tuple) {
			keys = tupleToIntArray((Tuple) keysArrayOrTuple);
		} else {
			keys = (int[]) keysArrayOrTuple;
		}
		DataSet op1 = (DataSet) sets.get(parentID);
		sets.put(setID, op1.partitionByHash(keys));

	}

	private void createJoinOperation(DatasizeHint mode, INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		if (info.types != null && (info.projections == null || info.projections.length == 0)) {
			sets.put(info.setID, applyJoinOperation(op1, op2, info.keys1, info.keys2, mode, info));
		} else {
			DefaultJoin defaultResult;
			switch (mode) {
				case NONE:
					defaultResult = op1.join(op2).where(info.keys1).equalTo(info.keys2);
					break;
				case HUGE:
					defaultResult = op1.joinWithHuge(op2).where(info.keys1).equalTo(info.keys2);
					break;
				case TINY:
					defaultResult = op1.joinWithTiny(op2).where(info.keys1).equalTo(info.keys2);
					break;
				default:
					throw new IllegalArgumentException("Invalid join mode specified.");
			}
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

	protected abstract DataSet applyJoinOperation(DataSet op1, DataSet op2, int[] firstKeys, int[] secondKeys, DatasizeHint mode, INFO info);

	private void createMapOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyMapOperation(op1, info));
	}

	protected abstract DataSet applyMapOperation(DataSet op1, INFO info);

	private void createMapPartitionOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyMapPartitionOperation(op1, info));
	}

	protected abstract DataSet applyMapPartitionOperation(DataSet op1, INFO info);

	protected void createProjectOperation() throws IOException {
		int setID = (Integer) receiver.getRecord(true);
		int parentID = (Integer) receiver.getRecord(true);
		Object keysArrayOrTuple = receiver.getRecord(true);
		int[] keys;
		if (keysArrayOrTuple instanceof Tuple) {
			keys = tupleToIntArray((Tuple) keysArrayOrTuple);
		} else {
			keys = (int[]) keysArrayOrTuple;
		}
		DataSet op1 = (DataSet) sets.get(parentID);
		sets.put(setID, op1.project(keys).name("Projection"));
	}

	private void createRebalanceOperation() throws IOException {
		int setID = (Integer) receiver.getRecord(true);
		int parentID = (Integer) receiver.getRecord(true);
		DataSet op = (DataSet) sets.get(parentID);
		sets.put(setID, op.rebalance().name("Rebalance"));
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

	protected abstract DataSet applyReduceOperation(DataSet op1, INFO info);

	protected abstract DataSet applyReduceOperation(UnsortedGrouping op1, INFO info);

	protected void createSortOperation() throws IOException {
		int setID = (Integer) receiver.getRecord(true);
		int parentID = (Integer) receiver.getRecord(true);
		int field = (Integer) receiver.getRecord(true);
		int encodedOrder = (Integer) receiver.getRecord(true);
		Order order;
		switch (encodedOrder) {
			case 0:
				order = Order.NONE;
				break;
			case 1:
				order = Order.ASCENDING;
				break;
			case 2:
				order = Order.DESCENDING;
				break;
			case 3:
				order = Order.ANY;
				break;
			default:
				order = Order.NONE;
				break;
		}
		Grouping op1 = (Grouping) sets.get(parentID);
		if (op1 instanceof UnsortedGrouping) {
			sets.put(setID, ((UnsortedGrouping) op1).sortGroup(field, order));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(setID, ((SortedGrouping) op1).sortGroup(field, order));
		}
	}

	protected void createUnionOperation() throws IOException {
		int setID = (Integer) receiver.getRecord(true);
		int parentID = (Integer) receiver.getRecord(true);
		int otherID = (Integer) receiver.getRecord(true);
		DataSet op1 = (DataSet) sets.get(parentID);
		DataSet op2 = (DataSet) sets.get(otherID);
		sets.put(setID, op1.union(op2).name("Union"));
	}

	//====Utility=======================================================================================================
	protected int[] tupleToIntArray(Tuple tuple) {
		int[] keys = new int[tuple.getArity()];
		for (int y = 0; y < tuple.getArity(); y++) {
			keys[y] = (Integer) tuple.getField(y);
		}
		return keys;
	}
}
