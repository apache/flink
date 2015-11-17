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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.CoGroupRawOperator;
import org.apache.flink.api.java.operators.CrossOperator.DefaultCross;
import org.apache.flink.api.java.operators.CrossOperator.ProjectCross;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.JoinOperator.ProjectJoin;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UdfOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.python.api.PythonOperationInfo.DatasizeHint;
import static org.apache.flink.python.api.PythonOperationInfo.DatasizeHint.HUGE;
import static org.apache.flink.python.api.PythonOperationInfo.DatasizeHint.NONE;
import static org.apache.flink.python.api.PythonOperationInfo.DatasizeHint.TINY;
import org.apache.flink.python.api.PythonOperationInfo.ProjectionEntry;
import org.apache.flink.python.api.functions.PythonCoGroup;
import org.apache.flink.python.api.functions.PythonCombineIdentity;
import org.apache.flink.python.api.functions.PythonMapPartition;
import org.apache.flink.python.api.streaming.Receiver;
import org.apache.flink.python.api.streaming.StreamPrinter;
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
	public static final String FLINK_PYTHON_PLAN_NAME = "/plan.py";

	public static final String FLINK_PYTHON2_BINARY_KEY = "python.binary.python2";
	public static final String FLINK_PYTHON3_BINARY_KEY = "python.binary.python3";
	public static final String PLANBINDER_CONFIG_BCVAR_COUNT = "PLANBINDER_BCVAR_COUNT";
	public static final String PLANBINDER_CONFIG_BCVAR_NAME_PREFIX = "PLANBINDER_BCVAR_";
	public static String FLINK_PYTHON2_BINARY_PATH = GlobalConfiguration.getString(FLINK_PYTHON2_BINARY_KEY, "python");
	public static String FLINK_PYTHON3_BINARY_PATH = GlobalConfiguration.getString(FLINK_PYTHON3_BINARY_KEY, "python3");

	private static final String FLINK_PYTHON_FILE_PATH = System.getProperty("java.io.tmpdir") + "/flink_plan";
	private static final String FLINK_PYTHON_REL_LOCAL_PATH = "/resources/python";
	private static final String FLINK_DIR = System.getenv("FLINK_ROOT_DIR");
	private static String FULL_PATH;

	public static StringBuilder arguments = new StringBuilder();

	private Process process;

	public static boolean usePython3 = false;

	private static String FLINK_HDFS_PATH = "hdfs:/tmp";
	public static final String FLINK_TMP_DATA_DIR = System.getProperty("java.io.tmpdir") + "/flink_data";

	public static boolean DEBUG = false;

	private HashMap<Integer, Object> sets = new HashMap();
	public ExecutionEnvironment env;
	private Receiver receiver;

	public static final int MAPPED_FILE_SIZE = 1024 * 1024 * 64;

	/**
	 * Entry point for the execution of a python plan.
	 *
	 * @param args planPath[ package1[ packageX[ - parameter1[ parameterX]]]]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: ./bin/pyflink<2/3>.sh <pathToScript>[ <pathToPackage1>[ <pathToPackageX]][ - <parameter1>[ <parameterX>]]");
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
				? FLINK_DIR + FLINK_PYTHON_REL_LOCAL_PATH //command-line
				: FileSystem.getLocalFileSystem().getWorkingDirectory().toString() //testing
				+ "/src/main/python/org/apache/flink/python/api";
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
			prepareFiles(Arrays.copyOfRange(args, 0, split == 0 ? 1 : split));
			startPython(Arrays.copyOfRange(args, split == 0 ? args.length : split + 1, args.length));
			receivePlan();

			if (env instanceof LocalEnvironment) {
				FLINK_HDFS_PATH = "file:" + System.getProperty("java.io.tmpdir") + "/flink";
			}

			distributeFiles(env);
			env.execute();
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
	private void prepareFiles(String... filePaths) throws IOException, URISyntaxException {
		//Flink python package
		String tempFilePath = FLINK_PYTHON_FILE_PATH;
		clearPath(tempFilePath);
		FileCache.copy(new Path(FULL_PATH), new Path(tempFilePath), false);

		//plan file		
		copyFile(filePaths[0], FLINK_PYTHON_PLAN_NAME);

		//additional files/folders
		for (int x = 1; x < filePaths.length; x++) {
			copyFile(filePaths[x], null);
		}
	}

	private static void clearPath(String path) throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new URI(path));
		if (fs.exists(new Path(path))) {
			fs.delete(new Path(path), true);
		}
	}

	private static void copyFile(String path, String name) throws IOException, URISyntaxException {
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		String identifier = name == null ? path.substring(path.lastIndexOf("/")) : name;
		String tmpFilePath = FLINK_PYTHON_FILE_PATH + "/" + identifier;
		clearPath(tmpFilePath);
		Path p = new Path(path);
		FileCache.copy(p.makeQualified(FileSystem.get(p.toUri())), new Path(tmpFilePath), true);
	}

	private static void distributeFiles(ExecutionEnvironment env) throws IOException, URISyntaxException {
		clearPath(FLINK_HDFS_PATH);
		FileCache.copy(new Path(FLINK_PYTHON_FILE_PATH), new Path(FLINK_HDFS_PATH), true);
		env.registerCachedFile(FLINK_HDFS_PATH, FLINK_PYTHON_DC_ID);
		clearPath(FLINK_PYTHON_FILE_PATH);
	}

	private void startPython(String[] args) throws IOException {
		for (String arg : args) {
			arguments.append(" ").append(arg);
		}
		receiver = new Receiver(null);
		receiver.open(FLINK_TMP_DATA_DIR + "/output");

		String pythonBinaryPath = usePython3 ? FLINK_PYTHON3_BINARY_PATH : FLINK_PYTHON2_BINARY_PATH;

		try {
			Runtime.getRuntime().exec(pythonBinaryPath);
		} catch (IOException ex) {
			throw new RuntimeException(pythonBinaryPath + " does not point to a valid python binary.");
		}
		process = Runtime.getRuntime().exec(pythonBinaryPath + " -B " + FLINK_PYTHON_FILE_PATH + FLINK_PYTHON_PLAN_NAME + arguments.toString());

		new StreamPrinter(process.getInputStream()).start();
		new StreamPrinter(process.getErrorStream()).start();

		try {
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}

		try {
			int value = process.exitValue();
			if (value != 0) {
				throw new RuntimeException("Plan file caused an error. Check log-files for details.");
			}
			if (value == 0) {
				throw new RuntimeException("Plan file exited prematurely without an error.");
			}
		} catch (IllegalThreadStateException ise) {//Process still running
		}

		process.getOutputStream().write("plan\n".getBytes());
		process.getOutputStream().write((FLINK_TMP_DATA_DIR + "/output\n").getBytes());
		process.getOutputStream().flush();
	}

	private void close() {
		try { //prevent throwing exception so that previous exceptions aren't hidden.
			if (!DEBUG) {
				FileSystem hdfs = FileSystem.get(new URI(FLINK_HDFS_PATH));
				hdfs.delete(new Path(FLINK_HDFS_PATH), true);
			}

			FileSystem local = FileSystem.getLocalFileSystem();
			local.delete(new Path(FLINK_PYTHON_FILE_PATH), true);
			local.delete(new Path(FLINK_TMP_DATA_DIR), true);
			receiver.close();
		} catch (NullPointerException npe) {
		} catch (IOException ioe) {
			LOG.error("PythonAPI file cleanup failed. " + ioe.getMessage());
		} catch (URISyntaxException use) { // can't occur
		}
		try {
			process.exitValue();
		} catch (NullPointerException npe) { //exception occurred before process was started
		} catch (IllegalThreadStateException ise) { //process still active
			process.destroy();
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
					env.setParallelism(dop);
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
		PROJECTION, SORT, UNION, FIRST, DISTINCT, GROUPBY, AGGREGATE,
		REBALANCE, PARTITION_HASH,
		BROADCAST,
		COGROUP, CROSS, CROSS_H, CROSS_T, FILTER, FLATMAP, GROUPREDUCE, JOIN, JOIN_H, JOIN_T, MAP, REDUCE, MAPPARTITION
	}

	private void receiveOperations() throws IOException {
		Integer operationCount = (Integer) receiver.getRecord(true);
		for (int x = 0; x < operationCount; x++) {
			String identifier = (String) receiver.getRecord();
			Operation op = null;
			try {
				op = Operation.valueOf(identifier.toUpperCase());
			} catch (IllegalArgumentException iae) {
				throw new IllegalArgumentException("Invalid operation specified: " + identifier);
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
					case COGROUP:
						createCoGroupOperation(createOperationInfo(op));
						break;
					case CROSS:
						createCrossOperation(NONE, createOperationInfo(op));
						break;
					case CROSS_H:
						createCrossOperation(HUGE, createOperationInfo(op));
						break;
					case CROSS_T:
						createCrossOperation(TINY, createOperationInfo(op));
						break;
					case FILTER:
						createFilterOperation(createOperationInfo(op));
						break;
					case FLATMAP:
						createFlatMapOperation(createOperationInfo(op));
						break;
					case GROUPREDUCE:
						createGroupReduceOperation(createOperationInfo(op));
						break;
					case JOIN:
						createJoinOperation(NONE, createOperationInfo(op));
						break;
					case JOIN_H:
						createJoinOperation(HUGE, createOperationInfo(op));
						break;
					case JOIN_T:
						createJoinOperation(TINY, createOperationInfo(op));
						break;
					case MAP:
						createMapOperation(createOperationInfo(op));
						break;
					case MAPPARTITION:
						createMapPartitionOperation(createOperationInfo(op));
						break;
					case REDUCE:
						createReduceOperation(createOperationInfo(op));
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
	private PythonOperationInfo createOperationInfo(Operation operationIdentifier) throws IOException {
		return new PythonOperationInfo(receiver, operationIdentifier);
	}

	private void createCsvSource(PythonOperationInfo info) throws IOException {
		if (!(info.types instanceof CompositeType)) {
			throw new RuntimeException("The output type of a csv source has to be a tuple or a "
					+ "pojo type. The derived type is " + info);
		}

		sets.put(info.setID, env.createInput(new CsvInputFormat(new Path(info.path),
				info.lineDelimiter, info.fieldDelimiter, (CompositeType) info.types), info.types)
				.name("CsvSource"));
	}

	private void createTextSource(PythonOperationInfo info) throws IOException {
		sets.put(info.setID, env.readTextFile(info.path).name("TextSource"));
	}

	private void createValueSource(PythonOperationInfo info) throws IOException {
		sets.put(info.setID, env.fromElements(info.values).name("ValueSource"));
	}

	private void createSequenceSource(PythonOperationInfo info) throws IOException {
		sets.put(info.setID, env.generateSequence(info.from, info.to).name("SequenceSource"));
	}

	private void createCsvSink(PythonOperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.writeAsCsv(info.path, info.lineDelimiter, info.fieldDelimiter, info.writeMode).name("CsvSink");
	}

	private void createTextSink(PythonOperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.writeAsText(info.path, info.writeMode).name("TextSink");
	}

	private void createPrintSink(PythonOperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.output(new PrintingOutputFormat(info.toError));
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

		sets.put(info.setID, ao.name("Aggregation"));
	}

	private void createDistinctOperation(PythonOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, info.keys.length == 0 ? op.distinct() : op.distinct(info.keys).name("Distinct"));
	}

	private void createFirstOperation(PythonOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.first(info.count).name("First"));
	}

	private void createGroupOperation(PythonOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.groupBy(info.keys));
	}

	private void createHashPartitionOperation(PythonOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.partitionByHash(info.keys));
	}

	private void createProjectOperation(PythonOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.project(info.fields).name("Projection"));
	}

	private void createRebalanceOperation(PythonOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.rebalance().name("Rebalance"));
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
		sets.put(info.setID, op1.union(op2).name("Union"));
	}

	private void createCoGroupOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.setID, new CoGroupRawOperator(
				op1,
				op2,
				new Keys.ExpressionKeys(info.keys1, op1.getType()),
				new Keys.ExpressionKeys(info.keys2, op2.getType()),
				new PythonCoGroup(info.setID, info.types),
				info.types, info.name));
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
		if (info.types != null && (info.projections == null || info.projections.length == 0)) {
			sets.put(info.setID, defaultResult.mapPartition(new PythonMapPartition(info.setID, info.types)).name(info.name));
		} else if (info.projections.length == 0) {
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

	private void createFilterOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new PythonMapPartition(info.setID, info.types)).name(info.name));
	}

	private void createFlatMapOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new PythonMapPartition(info.setID, info.types)).name(info.name));
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
		if (info.combine) {
			return op1.reduceGroup(new PythonCombineIdentity(info.setID * -1))
					.setCombinable(true).name("PythonCombine")
					.mapPartition(new PythonMapPartition(info.setID, info.types))
					.name(info.name);
		} else {
			return op1.reduceGroup(new PythonCombineIdentity())
					.setCombinable(false).name("PythonGroupReducePreStep")
					.mapPartition(new PythonMapPartition(info.setID, info.types))
					.name(info.name);
		}
	}

	private DataSet applyGroupReduceOperation(UnsortedGrouping op1, PythonOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new PythonCombineIdentity(info.setID * -1))
					.setCombinable(true).name("PythonCombine")
					.mapPartition(new PythonMapPartition(info.setID, info.types))
					.name(info.name);
		} else {
			return op1.reduceGroup(new PythonCombineIdentity())
					.setCombinable(false).name("PythonGroupReducePreStep")
					.mapPartition(new PythonMapPartition(info.setID, info.types))
					.name(info.name);
		}
	}

	private DataSet applyGroupReduceOperation(SortedGrouping op1, PythonOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new PythonCombineIdentity(info.setID * -1))
					.setCombinable(true).name("PythonCombine")
					.mapPartition(new PythonMapPartition(info.setID, info.types))
					.name(info.name);
		} else {
			return op1.reduceGroup(new PythonCombineIdentity())
					.setCombinable(false).name("PythonGroupReducePreStep")
					.mapPartition(new PythonMapPartition(info.setID, info.types))
					.name(info.name);
		}
	}

	private void createJoinOperation(DatasizeHint mode, PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		if (info.types != null && (info.projections == null || info.projections.length == 0)) {
			sets.put(info.setID, createDefaultJoin(op1, op2, info.keys1, info.keys2, mode).name("PythonJoinPreStep")
					.mapPartition(new PythonMapPartition(info.setID, info.types)).name(info.name));
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

	private DefaultJoin createDefaultJoin(DataSet op1, DataSet op2, String[] firstKeys, String[] secondKeys, DatasizeHint mode) {
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

	private void createMapOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new PythonMapPartition(info.setID, info.types)).name(info.name));
	}

	private void createMapPartitionOperation(PythonOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new PythonMapPartition(info.setID, info.types)).name(info.name));
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
		return op1.reduceGroup(new PythonCombineIdentity())
				.setCombinable(false).name("PythonReducePreStep")
				.mapPartition(new PythonMapPartition(info.setID, info.types))
				.name(info.name);
	}

	private DataSet applyReduceOperation(UnsortedGrouping op1, PythonOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new PythonCombineIdentity(info.setID * -1))
					.setCombinable(true).name("PythonCombine")
					.mapPartition(new PythonMapPartition(info.setID, info.types))
					.name(info.name);
		} else {
			return op1.reduceGroup(new PythonCombineIdentity())
					.setCombinable(false).name("PythonReducePreStep")
					.mapPartition(new PythonMapPartition(info.setID, info.types))
					.name(info.name);
		}
	}
}
