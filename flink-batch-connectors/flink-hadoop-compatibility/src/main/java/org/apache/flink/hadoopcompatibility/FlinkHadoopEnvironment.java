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

package org.apache.flink.hadoopcompatibility;

import org.apache.commons.cli.Option;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The FlinkHadoopEnvironment is the context in which a program is executed, connected with hadoop.
 *
 * The environment provides methods to interact with the hadoop cluster (data access).
 */
public final class FlinkHadoopEnvironment {
	// ----------------------------------- Hadoop Input Format ---------------------------------------

	private ExecutionEnvironment environment;

	public FlinkHadoopEnvironment(ExecutionEnvironment environment){
		this.environment = environment;
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapred.FileInputFormat}. The
	 * given inputName is set on the given job.
	 */
	@PublicEvolving
	public <K,V> DataSource<Tuple2<K, V>> readHadoopFile(org.apache.hadoop.mapred.FileInputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, String inputPath, JobConf job) {
		DataSource<Tuple2<K, V>> result = createHadoopInput(mapredInputFormat, key, value, job);

		org.apache.hadoop.mapred.FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(inputPath));

		return result;
	}

	/**
	 * Creates a {@link DataSet} from {@link org.apache.hadoop.mapred.SequenceFileInputFormat}
	 * A {@link org.apache.hadoop.mapred.JobConf} with the given inputPath is created.
	 */
	@PublicEvolving
	public <K,V> DataSource<Tuple2<K, V>> readSequenceFile(Class<K> key, Class<V> value, String inputPath) throws IOException {
		return readHadoopFile(new org.apache.hadoop.mapred.SequenceFileInputFormat<K, V>(), key, value, inputPath);
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapred.FileInputFormat}. A
	 * {@link org.apache.hadoop.mapred.JobConf} with the given inputPath is created.
	 */
	@PublicEvolving
	public <K,V> DataSource<Tuple2<K, V>> readHadoopFile(org.apache.hadoop.mapred.FileInputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, String inputPath) {
		return readHadoopFile(mapredInputFormat, key, value, inputPath, new JobConf());
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapred.InputFormat}.
	 */
	@PublicEvolving
	public <K,V> DataSource<Tuple2<K, V>> createHadoopInput(org.apache.hadoop.mapred.InputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, JobConf job) {
		HadoopInputFormat<K, V> hadoopInputFormat = new HadoopInputFormat<>(mapredInputFormat, key, value, job);

		return environment.createInput(hadoopInputFormat);
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}. The
	 * given inputName is set on the given job.
	 */
	@PublicEvolving
	public <K,V> DataSource<Tuple2<K, V>> readHadoopFile(org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K,V> mapreduceInputFormat, Class<K> key, Class<V> value, String inputPath, Job job) throws IOException {
		DataSource<Tuple2<K, V>> result = createHadoopInput(mapreduceInputFormat, key, value, job);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new org.apache
			.hadoop.fs.Path(inputPath));

		return result;
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}. A
	 * {@link org.apache.hadoop.mapreduce.Job} with the given inputPath is created.
	 */
	@PublicEvolving
	public <K,V> DataSource<Tuple2<K, V>> readHadoopFile(org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K,V> mapreduceInputFormat, Class<K> key, Class<V> value, String inputPath) throws IOException {
		return readHadoopFile(mapreduceInputFormat, key, value, inputPath, Job.getInstance());
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapreduce.InputFormat}.
	 */
	@PublicEvolving
	public <K,V> DataSource<Tuple2<K, V>> createHadoopInput(org.apache.hadoop.mapreduce.InputFormat<K,V> mapreduceInputFormat, Class<K> key, Class<V> value, Job job) {
		org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V> hadoopInputFormat = new org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<>(mapreduceInputFormat, key, value, job);

		return environment.createInput(hadoopInputFormat);
	}

	/**
	 * Returns {@link ParameterTool} for the arguments parsed by {@link GenericOptionsParser}
	 *
	 * @param args Input array arguments. It should be parsable by {@link GenericOptionsParser}
	 * @return A {@link ParameterTool}
	 * @throws IOException If arguments cannot be parsed by {@link GenericOptionsParser}
	 * @see GenericOptionsParser
	 */
	@PublicEvolving
	public static ParameterTool paramsFromGenericOptionsParser(String[] args) throws IOException {
		Option[] options = new GenericOptionsParser(args).getCommandLine().getOptions();
		Map<String, String> map = new HashMap<String, String>();
		for (Option option : options) {
			String[] split = option.getValue().split("=");
			map.put(split[0], split[1]);
		}
		return ParameterTool.fromMap(map);
	}
}
