/**
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

import org.apache.flink.api.java.functions.GroupReduceFunction;
import org.apache.flink.api.java.operators.translation.TupleUnwrappingIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.WritableTypeInfo;
import org.apache.flink.hadoopcompatibility.mapred.utils.HadoopConfiguration;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;

/**
 * The wrapper for a Hadoop Reducer (mapred API).
 */
public class HadoopReduceFunction<KEYIN extends WritableComparable, VALUEIN extends Writable,
		KEYOUT extends WritableComparable, VALUEOUT extends Writable> extends GroupReduceFunction<Tuple2<KEYIN,VALUEIN>,
		Tuple2<KEYOUT,VALUEOUT>> implements Serializable, ResultTypeQueryable<Tuple2<KEYOUT,VALUEOUT>> {

	private static final long serialVersionUID = 1L;

	private Class<KEYOUT> keyoutClass;
	private Class<VALUEOUT> valueoutClass;
	private JobConf jobConf;

	private Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> reducer;
	private Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> combiner;
	private HadoopOutputCollector outputCollector;
	private Reporter reporter;
	private ReducerTransformingIterator iterator;

	@SuppressWarnings("unchecked")
	public HadoopReduceFunction(JobConf jobConf) {
		this.jobConf = jobConf;

		this.reducer = InstantiationUtil.instantiate(jobConf.getReducerClass());
		final Class combinerClass = jobConf.getCombinerClass();
		if (combinerClass != null) {
			this.combiner = (Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) InstantiationUtil.instantiate(combinerClass);
		}

		this.keyoutClass = (Class<KEYOUT>) jobConf.getOutputKeyClass();
		this.valueoutClass = (Class<VALUEOUT>) jobConf.getOutputValueClass();
		this.outputCollector = new HadoopOutputCollector(keyoutClass, valueoutClass);
		this.iterator = new ReducerTransformingIterator();
		this.reporter = new HadoopDummyReporter();
	}

	/**
	 * A wrapping iterator for an iterator of key-value tuples that can be used as an iterator of values.
	 */
	private final class ReducerTransformingIterator extends TupleUnwrappingIterator<VALUEIN,KEYIN>
			implements java.io.Serializable {

		private static final long serialVersionUID = 1L;
		private Iterator<Tuple2<KEYIN,VALUEIN>> iterator;
		private KEYIN key;
		private Tuple2<KEYIN,VALUEIN> first;

		@Override()
		public void set(Iterator<Tuple2<KEYIN,VALUEIN>> iterator) {
			this.iterator = iterator;
			if(this.hasNext()) {
				this.first = iterator.next();
				this.key = this.first.f0;
			}
		}

		@Override
		public boolean hasNext() {
			if(this.first != null) {
				return true;
			}
			return iterator.hasNext();
		}

		@Override
		public VALUEIN next() {
			if(this.first != null) {
				final VALUEIN val = this.first.f1;
				this.first = null;
				return val;
			}
			final Tuple2<KEYIN,VALUEIN> tuple = iterator.next();
			return tuple.f1;
		}

		private KEYIN getKey() {
			return WritableUtils.clone(this.key, jobConf);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void reduce(Iterator<Tuple2<KEYIN,VALUEIN>> values, Collector<Tuple2<KEYOUT,VALUEOUT>> out)
			throws Exception {
		outputCollector.set(out);
		iterator.set(values);
		reducer.reduce(iterator.getKey(), iterator, outputCollector, reporter);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void combine(Iterator<Tuple2<KEYIN,VALUEIN>> values, Collector<Tuple2<KEYIN,VALUEIN>> out) throws Exception {
		if (this.combiner == null) {
			super.combine(values, out);
		}
		else {
			outputCollector.set(out);
			iterator.set(values);
			combiner.reduce(iterator.getKey(), iterator, outputCollector, reporter);
		}
	}

	@Override
	public TypeInformation<Tuple2<KEYOUT,VALUEOUT>> getProducedType() {
		final WritableTypeInfo<KEYOUT> keyTypeInfo = new WritableTypeInfo<KEYOUT>(keyoutClass);
		final WritableTypeInfo<VALUEOUT> valueTypleInfo = new WritableTypeInfo<VALUEOUT>(valueoutClass);
		return new TupleTypeInfo<Tuple2<KEYOUT,VALUEOUT>>(keyTypeInfo, valueTypleInfo);
	}

	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		jobConf.write(out);
		out.writeObject(iterator);
		out.writeObject(keyoutClass);
		out.writeObject(valueoutClass);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		jobConf = new JobConf();
		jobConf.readFields(in);
		try {
			this.reducer = (Reducer) InstantiationUtil.instantiate(jobConf.getReducerClass());
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop reducer", e);
		}
		iterator = (ReducerTransformingIterator) in.readObject();
		keyoutClass = (Class<KEYOUT>) in.readObject();
		valueoutClass = (Class<VALUEOUT>) in.readObject();

		final Class combinerClass = jobConf.getCombinerClass();
		if (combinerClass != null) {
			combiner = InstantiationUtil.instantiate(jobConf.getCombinerClass());
			combiner.configure(jobConf);
		}
		reducer.configure(jobConf);
		reducer = InstantiationUtil.instantiate(jobConf.getReducerClass());
		reducer.configure(jobConf);
		outputCollector = InstantiationUtil.instantiate(
				HadoopConfiguration.getOutputCollectorFromConf(jobConf));
		outputCollector.setExpectedKeyValueClasses(keyoutClass, valueoutClass);
		reporter = InstantiationUtil.instantiate(
				HadoopConfiguration.getReporterFromConf(jobConf));

	}
}
