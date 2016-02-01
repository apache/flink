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
package org.apache.flink.python.api;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.CoGroupRawOperator;
import org.apache.flink.api.java.operators.CrossOperator.DefaultCross;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UdfOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.python.api.functions.util.NestedKeyDiscarder;
import org.apache.flink.python.api.functions.util.StringTupleDeserializerMap;
import org.apache.flink.python.api.PythonOperationInfo.DatasizeHint;
import static org.apache.flink.python.api.PythonOperationInfo.DatasizeHint.HUGE;
import static org.apache.flink.python.api.PythonOperationInfo.DatasizeHint.NONE;
import static org.apache.flink.python.api.PythonOperationInfo.DatasizeHint.TINY;
import org.apache.flink.python.api.functions.PythonCoGroup;
import org.apache.flink.python.api.functions.util.IdentityGroupReduce;
import org.apache.flink.python.api.functions.PythonMapPartition;
import org.apache.flink.python.api.functions.util.KeyDiscarder;
import org.apache.flink.python.api.functions.util.SerializerMap;
import org.apache.flink.python.api.functions.util.StringDeserializerMap;
import org.apache.flink.python.api.streaming.plan.PythonPlanStreamer;
import org.apache.flink.runtime.filecache.FileCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows the execution of a Flink plan written in python.
 */
public class PythonPlanBinder {
	static final Logger LOG = LoggerFactory.getLogger(PythonPlanBinder.class);

	public static final String ARGUMENT_PYTHON_2 = "2";
	public static final String ARGUMENT_PYTHON_3 = "3";

	public static final String FLINK_PYTHON_DC_ID = "flink";
	public static final String FLINK_PYTHON_PLAN_NAME = File.separator + "plan.py";

	public static final String FLINK_PYTHON2_BINARY_KEY = "python.binary.python2";
	public static final String FLINK_PYTHON3_BINARY_KEY = "python.binary.python3";
	public static final String PLANBINDER_CONFIG_BCVAR_COUNT = "PLANBINDER_BCVAR_COUNT";
	public static final String PLANBINDER_CONFIG_BCVAR_NAME_PREFIX = "PLANBINDER_BCVAR_";
	public static String FLINK_PYTHON2_BINARY_PATH = GlobalConfiguration.getString(FLINK_PYTHON2_BINARY_KEY, "python");
	public static String FLINK_PYTHON3_BINARY_PATH = GlobalConfiguration.getString(FLINK_PYTHON3_BINARY_KEY, "python3");

	private static final Random r = new Random();

	public static final String FLINK_PYTHON_FILE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "flink_plan";
	private static final String FLINK_PYTHON_REL_LOCAL_PATH = File.separator + "resources" + File.separator + "python";
	private static final String FLINK_DIR = System.getenv("FLINK_ROOT_DIR");
	private static String FULL_PATH;

	public static StringBuilder arguments = new StringBuilder();

	public static boolean usePython3 = false;

	private static String FLINK_HDFS_PATH = "hdfs:/tmp";
	public static final String FLINK_TMP_DATA_DIR = System.getProperty("java.io.tmpdir") + File.separator + "flink_data";

	private HashMap<Integer, Object> sets = new HashMap();
	public ExecutionEnvironment env;
	private PythonPlanStreamer streamer;

	public static final int MAPPED_FILE_SIZE = 1024 * 1024 * 64;

	/**
	 * Entry point for the execution of a python plan.
	 *
	 * @param args planPath[ package1[ packageX[ - parameter1[ parameterX]]]]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: ./bin/pyflink<2/3>.[sh/bat] <pathToScript>[ <pathToPackage1>[ <pathToPackageX]][ - <parameter1>[ <parameterX>]]");
			return;
		}
		usePython3 = args[0].equals(ARGUMENT_PYTHON_3);
		PythonPlanBinder binder = new PythonPlanBinder();
		binder.runPlan(Arrays.copyOfRange(args, 1, args.length));
	}

	public PythonPlanBinder() throws IOException {
		FLINK_PYTHON2_BINARY_PATH = GlobalConfiguration.getString(FLINK_PYTHON2_BINARY_KEY, "python");
		FLINK_PYTHON3_BINARY_PATH = GlobalConfiguration.getString(FLINK_PYTHON3_BINARY_KEY, "python3");
		FULL_PATH = FLINK_DIR != null
				//command-line
				? FLINK_DIR + FLINK_PYTHON_REL_LOCAL_PATH
				//testing
				: new Path(FileSystem.getLocalFileSystem().getWorkingDirectory(), "src/main/python/org/apache/flink/python/api").toString();
	}

	private void runPlan(String[] args) throws Exception {
		env = ExecutionEnvironment.getExecutionEnvironment();

		int split = 0;
		for (int x = 0; x < args.length; x++) {
			if (args[x].compareTo("-") == 0) {
				split = x;
			}
		}

		try {
			String tmpPath = FLINK_PYTHON_FILE_PATH + r.nextInt();
			prepareFiles(tmpPath, Arrays.copyOfRange(args, 0, split == 0 ? 1 : split));
			startPython(tmpPath, Arrays.copyOfRange(args, split == 0 ? args.length : split + 1, args.length));
			receivePlan();

			if (env instanceof LocalEnvironment) {
				FLINK_HDFS_PATH = "file:" + System.getProperty("java.io.tmpdir") + File.separator + "flink";
			}

			distributeFiles(tmpPath, env);
			JobExecutionResult jer = env.execute();
			sendResult(jer);
			close();
		} catch (Exception e) {
			close();
			throw e;
		}
	}

	//=====Setup========================================================================================================
	/**
	 * Copies all files to a common directory (FLINK_PYTHON_FILE_PATH). This allows us to distribute it as one big
	 * package, and resolves PYTHONPATH issues.
	 *
	 * @param filePaths
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	private void prepareFiles(String tempFilePath, String... filePaths) throws IOException, URISyntaxException {
		//Flink python package
		clearPath(tempFilePath);
		FileCache.copy(new Path(FULL_PATH), new Path(tempFilePath), false);

		//plan file		
		copyFile(filePaths[0], tempFilePath, FLINK_PYTHON_PLAN_NAME);

		//additional files/folders
		for (int x = 1; x < filePaths.length; x++) {
			copyFile(filePaths[x], tempFilePath, null);
		}
	}

	private static void clearPath(String path) throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new Path(path).toUri());
		if (fs.exists(new Path(path))) {
			fs.delete(new Path(path), true);
		}
	}

	private static void copyFile(String path, String target, String name) throws IOException, URISyntaxException {
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		String identifier = name == null ? path.substring(path.lastIndexOf("/")) : name;
		String tmpFilePath = target + "/" + identifier;
		clearPath(tmpFilePath);
		Path p = new Path(path);
		FileCache.copy(p.makeQualified(FileSystem.get(p.toUri())), new Path(tmpFilePath), true);
	}

	private static void distributeFiles(String tmpPath, ExecutionEnvironment env) throws IOException, URISyntaxException {
		clearPath(FLINK_HDFS_PATH);
		FileCache.copy(new Path(tmpPath), new Path(FLINK_HDFS_PATH), true);
		env.registerCachedFile(FLINK_HDFS_PATH, FLINK_PYTHON_DC_ID);
		clearPath(tmpPath);
	}

	private void startPython(String tempPath, String[] args) throws IOException {
		for (String arg : args) {
			arguments.append(" ").append(arg);
		}
		streamer = new PythonPlanStreamer();
		streamer.open(tempPath, arguments.toString());
	}

	private void sendResult(JobExecutionResult jer) throws IOException {
		long runtime = jer.getNetRuntime();
		streamer.sendRecord(runtime);
	}

	private void close() {
		try { //prevent throwing exception so that previous exceptions aren't hidden.
			FileSystem hdfs = FileSystem.get(new URI(FLINK_HDFS_PATH));
			hdfs.delete(new Path(FLINK_HDFS_PATH), true);

			FileSystem local = FileSystem.getLocalFileSystem();
			local.delete(new Path(FLINK_PYTHON_FILE_PATH), true);
			local.delete(new Path(FLINK_TMP_DATA_DIR), true);
			streamer.close();
		} catch (NullPointerException npe) {
		} catch (IOException ioe) {
			LOG.error("PythonAPI file cleanup failed. " + ioe.getMessage());
		} catch (URISyntaxException use) { // can't occur
		}
	}

	//====Plan==========================================================================================================
	private void receivePlan() throws IOException {
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
		RETRY
	}

	private void receiveParameters() throws IOException {
		for (int x = 0; x < 3; x++) {
			Tuple value = (Tuple) streamer.getRecord(true);
			switch (Parameters.valueOf(((String) value.getField(0)).toUpperCase())) {
				case DOP:
					Integer dop = (Integer) value.getField(1);
					env.setParallelism(dop);
					break;
				case MODE:
					FLINK_HDFS_PATH = (Boolean) value.getField(1) ? "file:/tmp/flink" : "hdfs:/tmp/flink";
					break;
				case RETRY:
					int retry = (Integer) value.getField(1);
					env.setRestartStrategy(RestartStrategies.fixedDelayRestart(retry, 10000L));
					break;
			}
		}
		if (env.getParallelism() < 0) {
			env.setParallelism(1);
		}
	}

	//====Operations====================================================================================================
	/**
	 * This enum contains the identifiers for all supported DataSet operations.
	 */
	protected enum Operation {
		SOURCE_CSV, SOURCE_TEXT, SOURCE_VALUE, SOURCE_SEQ, SINK_CSV, SINK_TEXT, SINK_PRINT,
		SORT, UNION, FIRST, DISTINCT, GROUPBY, AGGREGATE,
		REBALANCE, PARTITION_HASH,
		BROADCAST,
		COGROUP, CROSS, CROSS_H, CROSS_T, FILTER, FLATMAP, GROUPREDUCE, JOIN, JOIN_H, JOIN_T, MAP, REDUCE, MAPPARTITION
	}

	private void receiveOperations() throws IOException {
		Integer operationCount = (Integer) streamer.getRecord(true);
		for (int x = 0; x < operationCount; x++) {
			PythonOperationInfo info = new PythonOperationInfo(streamer);
			Operation op;
			try {
				op = Operation.valueOf(info.identifier.toUpperCase());
			} catch (IllegalArgumentException iae) {
				throw new IllegalArgumentException("Invalid operation specified: " + info.identifier);
			}
			switch (op) {
				case SOURCE_CSV:
					createCsvSource(info);
					break;
				case SOURCE_TEXT:
					createTextSource(info);
					break;
				case SOURCE_VALUE:
					createValueSource(info);
					break;
				case SOURCE_SEQ:
					createSequenceSource(info);
					break;
				case SINK_CSV:
					createCsvSink(info);
					break;
				case SINK_TEXT:
					createTextSink(info);
					break;
				case SINK_PRINT:
					createPrintSink(info);
					break;
				case BROADCAST:
					createBroadcastVariable(info);
					break;
				case AGGREGATE:
					createAggregationOperation(info);
					break;
				case DISTINCT:
					createDistinctOperation(info);
					break;
				case FIRST:
					createFirstOperation(info);
					break;
				case PARTITION_HASH:
					createHashPartitionOperation(info);
					break;
				case REBALANCE:
					createRebalanceOperation(info);
					break;
				case GROUPBY:
					createGroupOperation(info);
					break;
				case SORT:
					createSortOperation(info);
					break;
				case UNION:
					createUnionOperation(info);
					break;
				case COGROUP:
					createCoGroupOperation(info);
					break;
				case CROSS:
					createCrossOperation(NONE, info);
					break;
				case CROSS_H:
					createCrossOperation(HUGE, info);
					break;
				case CROSS_T:
					createCrossOperation(TINY, info);
					break;
				case FILTER:
					createFilterOperation(info);
					break;
				case FLATMAP:
					createFlatMapOperation(info);
					break;
				case GROUPREDUCE:
					createGroupReduceOperation(info);
					break;
				case JOIN:
					createJoinOperation(NONE, info);
					break;
				case JOIN_H:
					createJoinOperation(HUGE, info);
					break;
				case JOIN_T:
					createJoinOperation(TINY, info);
					break;
				case MAP:
					createMapOperation(info);
					break;
				case MAPPARTITION:
					createMapPartitionOperation(info);
					break;
				case REDUCE:
					createReduceOperation(info);
					break;
			}
		}
	}

	private int getParallelism(PythonOperationInfo info) {
		return info.parallelism == -1 ? env.getParallelism() : info.parallelism;
	}

	private void createCsvSource(PythonOperationInfo info) throws IOException {
		if (!(info.types instanceof TupleTypeInfo)) {
			throw new RuntimeException("The output type of a csv source has to be a tuple. The derived type is " + info);
		}
		Path path = new Path(info.path);
		String lineD = info.lineDelimiter;
		String fieldD = info.fieldDelimiter;
		TupleTypeInfo<?> types = (TupleTypeInfo) info.types;
		sets.put(info.setID, env.createInput(new TupleCsvInputFormat(path, lineD, fieldD, types), info.types).setParallelism(getParallelism(info)).name("CsvSource")
				.map(new SerializerMap()).setParallelism(getParallelism(info)).name("CsvSourcePostStep"));
	}

	private void createTextSource(PythonOperationInfo info) throws IOException {
		sets.put(info.setID, env.readTextFile(info.path).setParallelism(getParallelism(info)).name("TextSource")
				.map(new SerializerMap()).setParallelism(getParallelism(info)).name("TextSourcePostStep"));
	}

	private void createValueSource(PythonOperationInfo info) throws IOException {
		sets.put(info.setID, env.fromElements(info.values).setParallelism(getParallelism(info)).name("ValueSource")
				.map(new SerializerMap()).setParallelism(getParallelism(info)).name("ValueSourcePostStep"));
	}

	private void createSequenceSource(PythonOperationInfo info) throws IOException {
		sets.put(info.setID, env.generateSequence(info.from, info.to).setParallelism(getParallelism(info)).name("SequenceSource")
				.map(new SerializerMap()).setParallelism(getParallelism(info)).name("SequenceSourcePostStep"));
	}

	private void createCsvSink(PythonOperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.map(new StringTupleDeserializerMap()).setParallelism(getParallelism(info)).name("CsvSinkPreStep")
				.writeAsCsv(info.path, info.lineDelimiter, info.fieldDelimiter, info.writeMode).setParallelism(getParallelism(info)).name("CsvSink");
	}

	private void createTextSink(PythonOperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.map(new StringDeserializerMap()).setParallelism(getParallelism(info))
			.writeAsText(info.path, info.writeMode).setParallelism(getParallelism(info)).name("TextSink");
	}

	private void createPrintSink(PythonOperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.map(new StringDeserializerMap()).setParallelism(getParallelism(info)).name("PrintSinkPreStep")
			.output(new PrintingOutputFormat(info.toError)).setParallelism(getParallelism(info));
	}

	private void createBroadcastVariable(PythonOperationInfo info) throws IOException {
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

	private void createAggregationOperation(PythonOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		AggregateOperator ao = op.aggregate(info.aggregates[0].agg, info.aggregates[0].field);

		for (int x = 1; x < info.count; x++) {
			ao = ao.and(info.aggregates[x].agg, info.aggregates[x].field);
		}

		sets.put(info.setID, ao.setParallelism(getParallelism(info)).name("Aggregation"));
	}

	private void createDistinctOperation(PythonOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.distinct(info.keys).setParallelism(getParallelism(info)).name("Distinct")
				.map(new KeyDiscarder()).setParallelism(getParallelism(info)).name("DistinctPostStep"));
	}

	private void createFirstOperation(PythonOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.first(info.count).setParallelism(getParallelism(info)).name("First"));
	}

	private void createGroupOperation(PythonOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.groupBy(info.keys));
	}

	private void createHashPartitionOperation(PythonOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.partitionByHash(info.keys).setParallelism(getParallelism(info))
				.map(new KeyDiscarder()).setParallelism(getParallelism(info)).name("HashPartitionPostStep"));
	}

	private void createRebalanceOperation(PythonOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.rebalance().setParallelism(getParallelism(info)).name("Rebalance"));
	}

	private void createSortOperation(PythonOperationInfo info) throws IOException {
		Grouping op1 = (Grouping) sets.get(info.parentID);
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, ((UnsortedGrouping) op1).sortGroup(info.field, info.order));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.setID, ((SortedGrouping) op1).sortGroup(info.field, info.order));
		}
	}

	private void createUnionOperation(PythonOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.setID, op1.union(op2).setParallelism(getParallelism(info)).name("Union"));
	}

	private void createCoGroupOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		Keys.ExpressionKeys<?> key1 = new Keys.ExpressionKeys(info.keys1, op1.getType());
		Keys.ExpressionKeys<?> key2 = new Keys.ExpressionKeys(info.keys2, op2.getType());
		PythonCoGroup pcg = new PythonCoGroup(info.setID, info.types);
		sets.put(info.setID, new CoGroupRawOperator(op1, op2, key1, key2, pcg, info.types, info.name).setParallelism(getParallelism(info)));
	}

	private void createCrossOperation(DatasizeHint mode, PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

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

		defaultResult.setParallelism(getParallelism(info));
		if (info.usesUDF) {
			sets.put(info.setID, defaultResult.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name));
		} else {
			sets.put(info.setID, defaultResult.name("DefaultCross"));
		}
	}

	private void createFilterOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name));
	}

	private void createFlatMapOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name));
	}

	private void createGroupReduceOperation(PythonOperationInfo info) {
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

	private DataSet applyGroupReduceOperation(DataSet op1, PythonOperationInfo info) {
		return op1.reduceGroup(new IdentityGroupReduce()).setCombinable(false).name("PythonGroupReducePreStep").setParallelism(getParallelism(info))
				.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name);
	}

	private DataSet applyGroupReduceOperation(UnsortedGrouping op1, PythonOperationInfo info) {
		return op1.reduceGroup(new IdentityGroupReduce()).setCombinable(false).setParallelism(getParallelism(info)).name("PythonGroupReducePreStep")
				.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name);
	}

	private DataSet applyGroupReduceOperation(SortedGrouping op1, PythonOperationInfo info) {
		return op1.reduceGroup(new IdentityGroupReduce()).setCombinable(false).setParallelism(getParallelism(info)).name("PythonGroupReducePreStep")
				.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name);
	}

	private void createJoinOperation(DatasizeHint mode, PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		if (info.usesUDF) {
			sets.put(info.setID, createDefaultJoin(op1, op2, info.keys1, info.keys2, mode, getParallelism(info))
					.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name));
		} else {
			sets.put(info.setID, createDefaultJoin(op1, op2, info.keys1, info.keys2, mode, getParallelism(info)));
		}
	}

	private DataSet createDefaultJoin(DataSet op1, DataSet op2, String[] firstKeys, String[] secondKeys, DatasizeHint mode, int parallelism) {
		switch (mode) {
			case NONE:
				return op1.join(op2).where(firstKeys).equalTo(secondKeys).setParallelism(parallelism)
					.map(new NestedKeyDiscarder()).setParallelism(parallelism).name("DefaultJoinPostStep");
			case HUGE:
				return op1.joinWithHuge(op2).where(firstKeys).equalTo(secondKeys).setParallelism(parallelism)
					.map(new NestedKeyDiscarder()).setParallelism(parallelism).name("DefaultJoinPostStep");
			case TINY:
				return op1.joinWithTiny(op2).where(firstKeys).equalTo(secondKeys).setParallelism(parallelism)
					.map(new NestedKeyDiscarder()).setParallelism(parallelism).name("DefaultJoinPostStep");
			default:
				throw new IllegalArgumentException("Invalid join mode specified.");
		}
	}

	private void createMapOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name));
	}

	private void createMapPartitionOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name));
	}

	private void createReduceOperation(PythonOperationInfo info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.setID, applyReduceOperation((DataSet) op1, info));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, applyReduceOperation((UnsortedGrouping) op1, info));
		}
	}

	private DataSet applyReduceOperation(DataSet op1, PythonOperationInfo info) {
		return op1.reduceGroup(new IdentityGroupReduce()).setCombinable(false).setParallelism(getParallelism(info)).name("PythonReducePreStep")
				.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name);
	}

	private DataSet applyReduceOperation(UnsortedGrouping op1, PythonOperationInfo info) {
		return op1.reduceGroup(new IdentityGroupReduce()).setCombinable(false).setParallelism(getParallelism(info)).name("PythonReducePreStep")
				.mapPartition(new PythonMapPartition(info.setID, info.types)).setParallelism(getParallelism(info)).name(info.name);
	}
}
