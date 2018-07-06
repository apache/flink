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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.operators.CoGroupRawOperator;
import org.apache.flink.api.java.operators.CrossOperator.DefaultCross;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UdfOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.python.api.PythonOperationInfo.DatasizeHint;
import org.apache.flink.python.api.functions.PythonCoGroup;
import org.apache.flink.python.api.functions.PythonMapPartition;
import org.apache.flink.python.api.functions.util.IdentityGroupReduce;
import org.apache.flink.python.api.functions.util.KeyDiscarder;
import org.apache.flink.python.api.functions.util.NestedKeyDiscarder;
import org.apache.flink.python.api.functions.util.SerializerMap;
import org.apache.flink.python.api.functions.util.StringDeserializerMap;
import org.apache.flink.python.api.functions.util.StringTupleDeserializerMap;
import org.apache.flink.python.api.streaming.plan.PythonPlanStreamer;
import org.apache.flink.python.api.util.SetCache;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.apache.flink.python.api.PythonOperationInfo.DatasizeHint.HUGE;
import static org.apache.flink.python.api.PythonOperationInfo.DatasizeHint.NONE;
import static org.apache.flink.python.api.PythonOperationInfo.DatasizeHint.TINY;

/**
 * This class allows the execution of a Flink plan written in python.
 */
public class PythonPlanBinder {
	static final Logger LOG = LoggerFactory.getLogger(PythonPlanBinder.class);

	public static final String FLINK_PYTHON_DC_ID = "flink";
	public static final String FLINK_PYTHON_PLAN_NAME = File.separator + "plan.py";

	public static final String PLANBINDER_CONFIG_BCVAR_COUNT = "PLANBINDER_BCVAR_COUNT";
	public static final String PLANBINDER_CONFIG_BCVAR_NAME_PREFIX = "PLANBINDER_BCVAR_";
	public static final String PLAN_ARGUMENTS_KEY = "python.plan.arguments";

	private final Configuration operatorConfig;

	private final String tmpPlanFilesDir;

	private final SetCache sets = new SetCache();
	private int currentEnvironmentID = 0;
	private PythonPlanStreamer streamer;

	/**
	 * Entry point for the execution of a python plan.
	 *
	 * @param args planPath[ package1[ packageX[ - parameter1[ parameterX]]]]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration globalConfig = GlobalConfiguration.loadConfiguration();
		PythonPlanBinder binder = new PythonPlanBinder(globalConfig);
		try {
			binder.runPlan(args);
		} catch (Exception e) {
			System.out.println("Failed to run plan: " + e.getMessage());
			LOG.error("Failed to run plan.", e);
		}
	}

	public PythonPlanBinder(Configuration globalConfig) {
		String configuredPlanTmpPath = globalConfig.getString(PythonOptions.PLAN_TMP_DIR);
		tmpPlanFilesDir = configuredPlanTmpPath != null
			? configuredPlanTmpPath
			: System.getProperty("java.io.tmpdir") + File.separator + "flink_plan_" + UUID.randomUUID();

		operatorConfig = new Configuration();
		operatorConfig.setString(PythonOptions.PYTHON_BINARY_PATH, globalConfig.getString(PythonOptions.PYTHON_BINARY_PATH));
		String configuredTmpDataDir = globalConfig.getString(PythonOptions.DATA_TMP_DIR);
		if (configuredTmpDataDir != null) {
			operatorConfig.setString(PythonOptions.DATA_TMP_DIR, configuredTmpDataDir);
		}
		operatorConfig.setLong(PythonOptions.MMAP_FILE_SIZE, globalConfig.getLong(PythonOptions.MMAP_FILE_SIZE));
	}

	void runPlan(String[] args) throws Exception {
		if (args.length < 1) {
			throw new IllegalArgumentException("Missing script file argument. Usage: ./bin/pyflink.[sh/bat] <pathToScript>[ <pathToPackage1>[ <pathToPackageX]][ - <parameter1>[ <parameterX>]]");
		}

		int split = 0;
		for (int x = 0; x < args.length; x++) {
			if (args[x].equals("-")) {
				split = x;
				break;
			}
		}

		try {
			String planFile = args[0];
			String[] filesToCopy = Arrays.copyOfRange(args, 1, split == 0 ? args.length : split);
			String[] planArgumentsArray = Arrays.copyOfRange(args, split == 0 ? args.length : split + 1, args.length);

			StringBuilder planArgumentsBuilder = new StringBuilder();
			for (String arg : planArgumentsArray) {
				planArgumentsBuilder.append(" ").append(arg);
			}
			String planArguments = planArgumentsBuilder.toString();

			operatorConfig.setString(PLAN_ARGUMENTS_KEY, planArguments);

			Path planPath = new Path(planFile);
			if (!FileSystem.getUnguardedFileSystem(planPath.toUri()).exists(planPath)) {
				throw new FileNotFoundException("Plan file " + planFile + " does not exist.");
			}
			for (String file : filesToCopy) {
				Path filePath = new Path(file);
				if (!FileSystem.getUnguardedFileSystem(filePath.toUri()).exists(filePath)) {
					throw new FileNotFoundException("Additional file " + file + " does not exist.");
				}
			}

			// setup temporary local directory for flink python library and user files
			Path targetDir = new Path(tmpPlanFilesDir);
			deleteIfExists(targetDir);
			targetDir.getFileSystem().mkdirs(targetDir);

			// extract and unzip flink library to temporary location
			unzipPythonLibrary(new Path(tmpPlanFilesDir));

			// copy user files to temporary location
			Path tmpPlanFilesPath = new Path(tmpPlanFilesDir);
			copyFile(planPath, tmpPlanFilesPath, FLINK_PYTHON_PLAN_NAME);
			for (String file : filesToCopy) {
				Path source = new Path(file);
				copyFile(source, tmpPlanFilesPath, source.getName());
			}

			// start python process
			streamer = new PythonPlanStreamer(operatorConfig);
			streamer.open(tmpPlanFilesDir, planArguments);

			// Python process should terminate itself when all jobs have been run
			while (streamer.preparePlanMode()) {
				ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				receivePlan(env);

				env.registerCachedFile(tmpPlanFilesPath.toUri().toString(), FLINK_PYTHON_DC_ID, true);

				JobExecutionResult jer = env.execute();
				long runtime = jer.getNetRuntime();
				streamer.sendRecord(runtime);

				streamer.finishPlanMode();
				sets.reset();
			}
		} finally {
			try {
				// clean up created files
				FileSystem local = FileSystem.getLocalFileSystem();
				local.delete(new Path(tmpPlanFilesDir), true);
			} catch (IOException ioe) {
				LOG.error("PythonAPI file cleanup failed. {}", ioe.getMessage());
			} finally {
				if (streamer != null) {
					streamer.close();
				}
			}
		}
	}

	private static void unzipPythonLibrary(Path targetDir) throws IOException {
		FileSystem targetFs = targetDir.getFileSystem();
		ClassLoader classLoader = PythonPlanBinder.class.getClassLoader();
		try (ZipInputStream zis = new ZipInputStream(classLoader.getResourceAsStream("python-source.zip"))) {
			ZipEntry entry = zis.getNextEntry();
			while (entry != null) {
				String fileName = entry.getName();
				Path newFile = new Path(targetDir, fileName);
				if (entry.isDirectory()) {
					targetFs.mkdirs(newFile);
				} else {
					try {
						LOG.debug("Unzipping to {}.", newFile);
						FSDataOutputStream fsDataOutputStream = targetFs.create(newFile, FileSystem.WriteMode.NO_OVERWRITE);
						IOUtils.copyBytes(zis, fsDataOutputStream, false);
					} catch (Exception e) {
						zis.closeEntry();
						throw new IOException("Failed to unzip flink python library.", e);
					}
				}

				zis.closeEntry();
				entry = zis.getNextEntry();
			}
			zis.closeEntry();
		}
	}

	//=====Setup========================================================================================================

	private static void deleteIfExists(Path path) throws IOException {
		FileSystem fs = path.getFileSystem();
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

	private static void copyFile(Path source, Path targetDirectory, String name) throws IOException {
		Path targetFilePath = new Path(targetDirectory, name);
		deleteIfExists(targetFilePath);
		FileUtils.copy(source, targetFilePath, true);
	}

	//====Plan==========================================================================================================
	private void receivePlan(ExecutionEnvironment env) throws IOException {
		//IDs used in HashMap of sets are only unique for each environment
		receiveParameters(env);
		receiveOperations(env);
	}

	//====Environment===================================================================================================

	/**
	 * This enum contains the identifiers for all supported environment parameters.
	 */
	private enum Parameters {
		DOP,
		RETRY,
		ID
	}

	private void receiveParameters(ExecutionEnvironment env) throws IOException {
		for (int x = 0; x < Parameters.values().length; x++) {
			Tuple value = (Tuple) streamer.getRecord(true);
			switch (Parameters.valueOf(((String) value.getField(0)).toUpperCase())) {
				case DOP:
					Integer dop = value.<Integer>getField(1);
					env.setParallelism(dop);
					break;
				case RETRY:
					int retry = value.<Integer>getField(1);
					env.setRestartStrategy(RestartStrategies.fixedDelayRestart(retry, 10000L));
					break;
				case ID:
					currentEnvironmentID = value.<Integer>getField(1);
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
		SORT, UNION, FIRST, DISTINCT, GROUPBY,
		REBALANCE, PARTITION_HASH,
		BROADCAST,
		COGROUP, CROSS, CROSS_H, CROSS_T, FILTER, FLATMAP, GROUPREDUCE, JOIN, JOIN_H, JOIN_T, MAP, REDUCE, MAPPARTITION
	}

	private void receiveOperations(ExecutionEnvironment env) throws IOException {
		Integer operationCount = (Integer) streamer.getRecord(true);
		for (int x = 0; x < operationCount; x++) {
			PythonOperationInfo info = new PythonOperationInfo(streamer, currentEnvironmentID);
			Operation op = Operation.valueOf(info.identifier.toUpperCase());
			switch (op) {
				case SOURCE_CSV:
					createCsvSource(env, info);
					break;
				case SOURCE_TEXT:
					createTextSource(env, info);
					break;
				case SOURCE_VALUE:
					createValueSource(env, info);
					break;
				case SOURCE_SEQ:
					createSequenceSource(env, info);
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
					createCoGroupOperation(info, info.types);
					break;
				case CROSS:
					createCrossOperation(NONE, info, info.types);
					break;
				case CROSS_H:
					createCrossOperation(HUGE, info, info.types);
					break;
				case CROSS_T:
					createCrossOperation(TINY, info, info.types);
					break;
				case FILTER:
					createFilterOperation(info, info.types);
					break;
				case FLATMAP:
					createFlatMapOperation(info, info.types);
					break;
				case GROUPREDUCE:
					createGroupReduceOperation(info);
					break;
				case JOIN:
					createJoinOperation(NONE, info, info.types);
					break;
				case JOIN_H:
					createJoinOperation(HUGE, info, info.types);
					break;
				case JOIN_T:
					createJoinOperation(TINY, info, info.types);
					break;
				case MAP:
					createMapOperation(info, info.types);
					break;
				case MAPPARTITION:
					createMapPartitionOperation(info, info.types);
					break;
				case REDUCE:
					createReduceOperation(info);
					break;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <T extends Tuple> void createCsvSource(ExecutionEnvironment env, PythonOperationInfo info) {
		if (!(info.types instanceof TupleTypeInfo)) {
			throw new RuntimeException("The output type of a csv source has to be a tuple. The derived type is " + info);
		}
		Path path = new Path(info.path);
		String lineD = info.lineDelimiter;
		String fieldD = info.fieldDelimiter;
		TupleTypeInfo<T> types = (TupleTypeInfo<T>) info.types;
		sets.add(info.setID, env.createInput(new TupleCsvInputFormat<>(path, lineD, fieldD, types), types).setParallelism(info.parallelism).name("CsvSource")
			.map(new SerializerMap<T>()).setParallelism(info.parallelism).name("CsvSourcePostStep"));
	}

	private void createTextSource(ExecutionEnvironment env, PythonOperationInfo info) {
		sets.add(info.setID, env.readTextFile(info.path).setParallelism(info.parallelism).name("TextSource")
			.map(new SerializerMap<String>()).setParallelism(info.parallelism).name("TextSourcePostStep"));
	}

	private void createValueSource(ExecutionEnvironment env, PythonOperationInfo info) {
		sets.add(info.setID, env.fromCollection(info.values).setParallelism(info.parallelism).name("ValueSource")
			.map(new SerializerMap<>()).setParallelism(info.parallelism).name("ValueSourcePostStep"));
	}

	private void createSequenceSource(ExecutionEnvironment env, PythonOperationInfo info) {
		sets.add(info.setID, env.generateSequence(info.frm, info.to).setParallelism(info.parallelism).name("SequenceSource")
			.map(new SerializerMap<Long>()).setParallelism(info.parallelism).name("SequenceSourcePostStep"));
	}

	private void createCsvSink(PythonOperationInfo info) {
		DataSet<byte[]> parent = sets.getDataSet(info.parentID);
		parent.map(new StringTupleDeserializerMap()).setParallelism(info.parallelism).name("CsvSinkPreStep")
				.writeAsCsv(info.path, info.lineDelimiter, info.fieldDelimiter, info.writeMode).setParallelism(info.parallelism).name("CsvSink");
	}

	private void createTextSink(PythonOperationInfo info) {
		DataSet<byte[]> parent = sets.getDataSet(info.parentID);
		parent.map(new StringDeserializerMap()).setParallelism(info.parallelism)
			.writeAsText(info.path, info.writeMode).setParallelism(info.parallelism).name("TextSink");
	}

	private void createPrintSink(PythonOperationInfo info) {
		DataSet<byte[]> parent = sets.getDataSet(info.parentID);
		parent.map(new StringDeserializerMap()).setParallelism(info.parallelism).name("PrintSinkPreStep")
			.output(new PrintingOutputFormat<String>(info.toError)).setParallelism(info.parallelism);
	}

	private void createBroadcastVariable(PythonOperationInfo info) {
		UdfOperator<?> op1 = (UdfOperator<?>) sets.getDataSet(info.parentID);
		DataSet<?> op2 = sets.getDataSet(info.otherID);

		op1.withBroadcastSet(op2, info.name);
		Configuration c = op1.getParameters();

		if (c == null) {
			c = new Configuration();
		}

		int count = c.getInteger(PLANBINDER_CONFIG_BCVAR_COUNT, 0);
		c.setInteger(PLANBINDER_CONFIG_BCVAR_COUNT, count + 1);
		c.setString(PLANBINDER_CONFIG_BCVAR_NAME_PREFIX + count, info.name);

		op1.withParameters(c);
	}

	private <K extends Tuple> void createDistinctOperation(PythonOperationInfo info) {
		DataSet<Tuple2<K, byte[]>> op = sets.getDataSet(info.parentID);
		DataSet<byte[]> result = op
			.distinct(info.keys.toArray(new String[info.keys.size()])).setParallelism(info.parallelism).name("Distinct")
			.map(new KeyDiscarder<K>()).setParallelism(info.parallelism).name("DistinctPostStep");
		sets.add(info.setID, result);
	}

	private <K extends Tuple> void createFirstOperation(PythonOperationInfo info) {
		if (sets.isDataSet(info.parentID)) {
			DataSet<byte[]> op = sets.getDataSet(info.parentID);
			sets.add(info.setID, op
				.first(info.count).setParallelism(info.parallelism).name("First"));
		} else if (sets.isUnsortedGrouping(info.parentID)) {
			UnsortedGrouping<Tuple2<K, byte[]>> op = sets.getUnsortedGrouping(info.parentID);
			sets.add(info.setID, op
				.first(info.count).setParallelism(info.parallelism).name("First")
				.map(new KeyDiscarder<K>()).setParallelism(info.parallelism).name("FirstPostStep"));
		} else if (sets.isSortedGrouping(info.parentID)) {
			SortedGrouping<Tuple2<K, byte[]>> op = sets.getSortedGrouping(info.parentID);
			sets.add(info.setID, op
				.first(info.count).setParallelism(info.parallelism).name("First")
				.map(new KeyDiscarder<K>()).setParallelism(info.parallelism).name("FirstPostStep"));
		}
	}

	private void createGroupOperation(PythonOperationInfo info) {
		DataSet<?> op1 = sets.getDataSet(info.parentID);
		sets.add(info.setID, op1.groupBy(info.keys.toArray(new String[info.keys.size()])));
	}

	private <K extends Tuple> void createHashPartitionOperation(PythonOperationInfo info) {
		DataSet<Tuple2<K, byte[]>> op1 = sets.getDataSet(info.parentID);
		DataSet<byte[]> result = op1
			.partitionByHash(info.keys.toArray(new String[info.keys.size()])).setParallelism(info.parallelism)
			.map(new KeyDiscarder<K>()).setParallelism(info.parallelism).name("HashPartitionPostStep");
		sets.add(info.setID, result);
	}

	private void createRebalanceOperation(PythonOperationInfo info) {
		DataSet<?> op = sets.getDataSet(info.parentID);
		sets.add(info.setID, op.rebalance().setParallelism(info.parallelism).name("Rebalance"));
	}

	private void createSortOperation(PythonOperationInfo info) {
		if (sets.isDataSet(info.parentID)) {
			throw new IllegalArgumentException("sort() can not be applied on a DataSet.");
		} else if (sets.isUnsortedGrouping(info.parentID)) {
			sets.add(info.setID, sets.getUnsortedGrouping(info.parentID).sortGroup(info.field, info.order));
		} else if (sets.isSortedGrouping(info.parentID)) {
			sets.add(info.setID, sets.getSortedGrouping(info.parentID).sortGroup(info.field, info.order));
		}
	}

	private <IN> void createUnionOperation(PythonOperationInfo info) {
		DataSet<IN> op1 = sets.getDataSet(info.parentID);
		DataSet<IN> op2 = sets.getDataSet(info.otherID);
		sets.add(info.setID, op1.union(op2).setParallelism(info.parallelism).name("Union"));
	}

	private <IN1, IN2, OUT> void createCoGroupOperation(PythonOperationInfo info, TypeInformation<OUT> type) {
		DataSet<IN1> op1 = sets.getDataSet(info.parentID);
		DataSet<IN2> op2 = sets.getDataSet(info.otherID);
		Keys.ExpressionKeys<IN1> key1 = new Keys.ExpressionKeys<>(info.keys1.toArray(new String[info.keys1.size()]), op1.getType());
		Keys.ExpressionKeys<IN2> key2 = new Keys.ExpressionKeys<>(info.keys2.toArray(new String[info.keys2.size()]), op2.getType());
		PythonCoGroup<IN1, IN2, OUT> pcg = new PythonCoGroup<>(operatorConfig, info.envID, info.setID, type);
		sets.add(info.setID, new CoGroupRawOperator<>(op1, op2, key1, key2, pcg, type, info.name).setParallelism(info.parallelism));
	}

	private <IN1, IN2, OUT> void createCrossOperation(DatasizeHint mode, PythonOperationInfo info, TypeInformation<OUT> type) {
		DataSet<IN1> op1 = sets.getDataSet(info.parentID);
		DataSet<IN2> op2 = sets.getDataSet(info.otherID);

		DefaultCross<IN1, IN2> defaultResult;
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

		defaultResult.setParallelism(info.parallelism);
		if (info.usesUDF) {
			sets.add(info.setID, defaultResult
				.mapPartition(new PythonMapPartition<Tuple2<IN1, IN2>, OUT>(operatorConfig, info.envID, info.setID, type))
				.setParallelism(info.parallelism).name(info.name));
		} else {
			sets.add(info.setID, defaultResult.name("DefaultCross"));
		}
	}

	private <IN, OUT> void createFilterOperation(PythonOperationInfo info, TypeInformation<OUT> type) {
		DataSet<IN> op1 = sets.getDataSet(info.parentID);
		sets.add(info.setID, op1
			.mapPartition(new PythonMapPartition<IN, OUT>(operatorConfig, info.envID, info.setID, type))
			.setParallelism(info.parallelism).name(info.name));
	}

	private <IN, OUT> void createFlatMapOperation(PythonOperationInfo info, TypeInformation<OUT> type) {
		DataSet<IN> op1 = sets.getDataSet(info.parentID);
		sets.add(info.setID, op1
			.mapPartition(new PythonMapPartition<IN, OUT>(operatorConfig, info.envID, info.setID, type))
			.setParallelism(info.parallelism).name(info.name));
	}

	private void createGroupReduceOperation(PythonOperationInfo info) {
		if (sets.isDataSet(info.parentID)) {
			sets.add(info.setID, applyGroupReduceOperation(sets.getDataSet(info.parentID), info, info.types));
		} else if (sets.isUnsortedGrouping(info.parentID)) {
			sets.add(info.setID, applyGroupReduceOperation(sets.getUnsortedGrouping(info.parentID), info, info.types));
		} else if (sets.isSortedGrouping(info.parentID)) {
			sets.add(info.setID, applyGroupReduceOperation(sets.getSortedGrouping(info.parentID), info, info.types));
		}
	}

	private <IN, OUT> DataSet<OUT> applyGroupReduceOperation(DataSet<IN> op1, PythonOperationInfo info, TypeInformation<OUT> type) {
		return op1
			.reduceGroup(new IdentityGroupReduce<IN>()).setCombinable(false).name("PythonGroupReducePreStep").setParallelism(info.parallelism)
			.mapPartition(new PythonMapPartition<IN, OUT>(operatorConfig, info.envID, info.setID, type))
			.setParallelism(info.parallelism).name(info.name);
	}

	private <IN, OUT> DataSet<OUT> applyGroupReduceOperation(UnsortedGrouping<IN> op1, PythonOperationInfo info, TypeInformation<OUT> type) {
		return op1
			.reduceGroup(new IdentityGroupReduce<IN>()).setCombinable(false).setParallelism(info.parallelism).name("PythonGroupReducePreStep")
			.mapPartition(new PythonMapPartition<IN, OUT>(operatorConfig, info.envID, info.setID, type))
			.setParallelism(info.parallelism).name(info.name);
	}

	private <IN, OUT> DataSet<OUT> applyGroupReduceOperation(SortedGrouping<IN> op1, PythonOperationInfo info, TypeInformation<OUT> type) {
		return op1
			.reduceGroup(new IdentityGroupReduce<IN>()).setCombinable(false).setParallelism(info.parallelism).name("PythonGroupReducePreStep")
			.mapPartition(new PythonMapPartition<IN, OUT>(operatorConfig, info.envID, info.setID, type))
			.setParallelism(info.parallelism).name(info.name);
	}

	private <IN1, IN2, OUT> void createJoinOperation(DatasizeHint mode, PythonOperationInfo info, TypeInformation<OUT> type) {
		DataSet<IN1> op1 = sets.getDataSet(info.parentID);
		DataSet<IN2> op2 = sets.getDataSet(info.otherID);

		if (info.usesUDF) {
			sets.add(info.setID, createDefaultJoin(op1, op2, info.keys1, info.keys2, mode, info.parallelism)
				.mapPartition(new PythonMapPartition<Tuple2<byte[], byte[]>, OUT>(operatorConfig, info.envID, info.setID, type))
				.setParallelism(info.parallelism).name(info.name));
		} else {
			sets.add(info.setID, createDefaultJoin(op1, op2, info.keys1, info.keys2, mode, info.parallelism));
		}
	}

	private <IN1, IN2> DataSet<Tuple2<byte[], byte[]>> createDefaultJoin(DataSet<IN1> op1, DataSet<IN2> op2, List<String> firstKeys, List<String> secondKeys, DatasizeHint mode, int parallelism) {
		String[] firstKeysArray = firstKeys.toArray(new String[firstKeys.size()]);
		String[] secondKeysArray = secondKeys.toArray(new String[secondKeys.size()]);
		switch (mode) {
			case NONE:
				return op1
					.join(op2).where(firstKeysArray).equalTo(secondKeysArray).setParallelism(parallelism)
					.map(new NestedKeyDiscarder<Tuple2<IN1, IN2>>()).setParallelism(parallelism).name("DefaultJoinPostStep");
			case HUGE:
				return op1
					.joinWithHuge(op2).where(firstKeysArray).equalTo(secondKeysArray).setParallelism(parallelism)
					.map(new NestedKeyDiscarder<Tuple2<IN1, IN2>>()).setParallelism(parallelism).name("DefaultJoinPostStep");
			case TINY:
				return op1
					.joinWithTiny(op2).where(firstKeysArray).equalTo(secondKeysArray).setParallelism(parallelism)
					.map(new NestedKeyDiscarder<Tuple2<IN1, IN2>>()).setParallelism(parallelism).name("DefaultJoinPostStep");
			default:
				throw new IllegalArgumentException("Invalid join mode specified.");
		}
	}

	private <IN, OUT> void createMapOperation(PythonOperationInfo info, TypeInformation<OUT> type) {
		DataSet<IN> op1 = sets.getDataSet(info.parentID);
		sets.add(info.setID, op1
			.mapPartition(new PythonMapPartition<IN, OUT>(operatorConfig, info.envID, info.setID, type))
			.setParallelism(info.parallelism).name(info.name));
	}

	private <IN, OUT> void createMapPartitionOperation(PythonOperationInfo info, TypeInformation<OUT> type) {
		DataSet<IN> op1 = sets.getDataSet(info.parentID);
		sets.add(info.setID, op1
			.mapPartition(new PythonMapPartition<IN, OUT>(operatorConfig, info.envID, info.setID, type))
			.setParallelism(info.parallelism).name(info.name));
	}

	private void createReduceOperation(PythonOperationInfo info) {
		if (sets.isDataSet(info.parentID)) {
			sets.add(info.setID, applyReduceOperation(sets.getDataSet(info.parentID), info, info.types));
		} else if (sets.isUnsortedGrouping(info.parentID)) {
			sets.add(info.setID, applyReduceOperation(sets.getUnsortedGrouping(info.parentID), info, info.types));
		} else if (sets.isSortedGrouping(info.parentID)) {
			throw new IllegalArgumentException("Reduce cannot be applied on a SortedGrouping.");
		}
	}

	private <IN, OUT> DataSet<OUT> applyReduceOperation(DataSet<IN> op1, PythonOperationInfo info, TypeInformation<OUT> type) {
		return op1
			.reduceGroup(new IdentityGroupReduce<IN>()).setCombinable(false).setParallelism(info.parallelism).name("PythonReducePreStep")
			.mapPartition(new PythonMapPartition<IN, OUT>(operatorConfig, info.envID, info.setID, type))
			.setParallelism(info.parallelism).name(info.name);
	}

	private <IN, OUT> DataSet<OUT> applyReduceOperation(UnsortedGrouping<IN> op1, PythonOperationInfo info, TypeInformation<OUT> type) {
		return op1
			.reduceGroup(new IdentityGroupReduce<IN>()).setCombinable(false).setParallelism(info.parallelism).name("PythonReducePreStep")
			.mapPartition(new PythonMapPartition<IN, OUT>(operatorConfig, info.envID, info.setID, type))
			.setParallelism(info.parallelism).name(info.name);
	}
}
