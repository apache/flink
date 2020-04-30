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

package org.apache.flink.hadoopcompatibility.mapred;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopTupleUnwrappingIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * This wrapper maps a Hadoop Reducer and Combiner (mapred API) to a combinable Flink GroupReduceFunction.
 */
@SuppressWarnings("rawtypes")
@Public
public final class HadoopReduceCombineFunction<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	extends RichGroupReduceFunction<Tuple2<KEYIN, VALUEIN>, Tuple2<KEYOUT, VALUEOUT>>
	implements GroupCombineFunction<Tuple2<KEYIN, VALUEIN>, Tuple2<KEYIN, VALUEIN>>,
				ResultTypeQueryable<Tuple2<KEYOUT, VALUEOUT>>, Serializable {

	private static final long serialVersionUID = 1L;

	private transient Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer;
	private transient Reducer<KEYIN, VALUEIN, KEYIN, VALUEIN> combiner;
	private transient JobConf jobConf;

	private transient HadoopTupleUnwrappingIterator<KEYIN, VALUEIN> valueIterator;
	private transient HadoopOutputCollector<KEYOUT, VALUEOUT> reduceCollector;
	private transient HadoopOutputCollector<KEYIN, VALUEIN> combineCollector;
	private transient Reporter reporter;

	/**
	 * Maps two Hadoop Reducer (mapred API) to a combinable Flink GroupReduceFunction.
	 *
	 * @param hadoopReducer The Hadoop Reducer that is mapped to a GroupReduceFunction.
	 * @param hadoopCombiner The Hadoop Reducer that is mapped to the combiner function.
	 */
	public HadoopReduceCombineFunction(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopReducer,
										Reducer<KEYIN, VALUEIN, KEYIN, VALUEIN> hadoopCombiner) {
		this(hadoopReducer, hadoopCombiner, new JobConf());
	}

	/**
	 * Maps two Hadoop Reducer (mapred API) to a combinable Flink GroupReduceFunction.
	 *
	 * @param hadoopReducer The Hadoop Reducer that is mapped to a GroupReduceFunction.
	 * @param hadoopCombiner The Hadoop Reducer that is mapped to the combiner function.
	 * @param conf The JobConf that is used to configure both Hadoop Reducers.
	 */
	public HadoopReduceCombineFunction(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopReducer,
								Reducer<KEYIN, VALUEIN, KEYIN, VALUEIN> hadoopCombiner, JobConf conf) {
		if (hadoopReducer == null) {
			throw new NullPointerException("Reducer may not be null.");
		}
		if (hadoopCombiner == null) {
			throw new NullPointerException("Combiner may not be null.");
		}
		if (conf == null) {
			throw new NullPointerException("JobConf may not be null.");
		}

		this.reducer = hadoopReducer;
		this.combiner = hadoopCombiner;
		this.jobConf = conf;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.reducer.configure(jobConf);
		this.combiner.configure(jobConf);

		this.reporter = new HadoopDummyReporter();
		Class<KEYIN> inKeyClass = (Class<KEYIN>) TypeExtractor.getParameterType(Reducer.class, reducer.getClass(), 0);
		TypeSerializer<KEYIN> keySerializer = TypeExtractor.getForClass(inKeyClass).createSerializer(getRuntimeContext().getExecutionConfig());
		this.valueIterator = new HadoopTupleUnwrappingIterator<>(keySerializer);
		this.combineCollector = new HadoopOutputCollector<>();
		this.reduceCollector = new HadoopOutputCollector<>();
	}

	@Override
	public void reduce(final Iterable<Tuple2<KEYIN, VALUEIN>> values, final Collector<Tuple2<KEYOUT, VALUEOUT>> out)
			throws Exception {
		reduceCollector.setFlinkCollector(out);
		valueIterator.set(values.iterator());
		reducer.reduce(valueIterator.getCurrentKey(), valueIterator, reduceCollector, reporter);
	}

	@Override
	public void combine(final Iterable<Tuple2<KEYIN, VALUEIN>> values, final Collector<Tuple2<KEYIN, VALUEIN>> out) throws Exception {
		combineCollector.setFlinkCollector(out);
		valueIterator.set(values.iterator());
		combiner.reduce(valueIterator.getCurrentKey(), valueIterator, combineCollector, reporter);
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeInformation<Tuple2<KEYOUT, VALUEOUT>> getProducedType() {
		Class<KEYOUT> outKeyClass = (Class<KEYOUT>) TypeExtractor.getParameterType(Reducer.class, reducer.getClass(), 2);
		Class<VALUEOUT> outValClass = (Class<VALUEOUT>) TypeExtractor.getParameterType(Reducer.class, reducer.getClass(), 3);

		final TypeInformation<KEYOUT> keyTypeInfo = TypeExtractor.getForClass(outKeyClass);
		final TypeInformation<VALUEOUT> valueTypleInfo = TypeExtractor.getForClass(outValClass);
		return new TupleTypeInfo<>(keyTypeInfo, valueTypleInfo);
	}

	/**
	 * Custom serialization methods.
	 * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html">http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html</a>
	 */
	private void writeObject(final ObjectOutputStream out) throws IOException {

		out.writeObject(reducer.getClass());
		out.writeObject(combiner.getClass());
		jobConf.write(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {

		Class<Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> reducerClass =
				(Class<Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>) in.readObject();
		reducer = InstantiationUtil.instantiate(reducerClass);

		Class<Reducer<KEYIN, VALUEIN, KEYIN, VALUEIN>> combinerClass =
				(Class<Reducer<KEYIN, VALUEIN, KEYIN, VALUEIN>>) in.readObject();
		combiner = InstantiationUtil.instantiate(combinerClass);

		jobConf = new JobConf();
		jobConf.readFields(in);
	}

}
