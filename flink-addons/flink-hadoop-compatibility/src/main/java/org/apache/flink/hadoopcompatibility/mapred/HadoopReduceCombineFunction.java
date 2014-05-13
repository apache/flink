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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.translation.TupleUnwrappingIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.WritableTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.mapred.utils.HadoopConfiguration;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * The wrapper for a Hadoop Reducer (mapred API). Parses a Hadoop JobConf object and initialises all operations related
 * reducers and combiners.
 */
@org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable
public final class HadoopReduceCombineFunction<KEYIN extends WritableComparable<?>, VALUEIN extends Writable,
										KEYOUT extends WritableComparable<?>, VALUEOUT extends Writable> 
					extends RichGroupReduceFunction<Tuple2<KEYIN,VALUEIN>,Tuple2<KEYOUT,VALUEOUT>> 
					implements ResultTypeQueryable<Tuple2<KEYOUT,VALUEOUT>>, Serializable {

	private static final long serialVersionUID = 1L;

	private transient Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> reducer;
	private transient Reducer<KEYIN,VALUEIN,KEYIN,VALUEIN> combiner;
	private transient JobConf jobConf;
	
	private transient Class<KEYOUT> keyOutClass;
	private transient Class<VALUEOUT> valOutClass;
	
	private transient HadoopOutputCollector<KEYIN,VALUEIN> combineCollector;
	private transient HadoopOutputCollector<KEYOUT,VALUEOUT> reduceCollector;
	private transient Reporter reporter;
	private transient ReducerTransformingIterator iterator;

	public HadoopReduceCombineFunction(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopReducer,
								Reducer<KEYIN,VALUEIN,KEYIN,VALUEIN> hadoopCombiner,
								Class<KEYOUT> keyOutClass, Class<VALUEOUT> valOutClass) {
		this(hadoopReducer, hadoopCombiner, keyOutClass, valOutClass, new JobConf());
	}
	
	public HadoopReduceCombineFunction(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopReducer,
								Reducer<KEYIN,VALUEIN,KEYIN,VALUEIN> hadoopCombiner,
								Class<KEYOUT> keyOutClass, Class<VALUEOUT> valOutClass, JobConf conf) {
		this.reducer = hadoopReducer;
		this.combiner = hadoopCombiner;

		this.keyOutClass = keyOutClass;
		this.valOutClass = valOutClass;
		
		this.jobConf = new JobConf();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		this.reporter = new HadoopDummyReporter();
		this.combineCollector = new HadoopOutputCollector<KEYIN, VALUEIN>();
		this.reduceCollector = new HadoopOutputCollector<KEYOUT, VALUEOUT>();
		this.iterator = new ReducerTransformingIterator();
	}

	/**
	 * A wrapping iterator for an iterator of key-value pairs that can be used as an iterator of values.
	 */
	private final class ReducerTransformingIterator extends TupleUnwrappingIterator<VALUEIN,KEYIN>
			implements java.io.Serializable {

		private static final long serialVersionUID = 1L;
		private Iterator<Tuple2<KEYIN,VALUEIN>> iterator;
		private KEYIN key;
		private Tuple2<KEYIN,VALUEIN> first;

		/**
		 * Set the iterator to wrap.
		 * @param iterator iterator to wrap
		 */
		@Override()
		public void set(final Iterator<Tuple2<KEYIN,VALUEIN>> iterator) {
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

	/**
	 * Wrap a hadoop reduce() function call and use a Flink collector to collect the result values.
	 * @param values The iterator returning the group of values to be reduced.
	 * @param out The collector to emit the returned values.
	 *
	 * @throws Exception
	 */
	@Override
	public void reduce(final Iterable<Tuple2<KEYIN,VALUEIN>> values, final Collector<Tuple2<KEYOUT,VALUEOUT>> out)
			throws Exception {
		reduceCollector.setFlinkCollector(out);
		iterator.set(values.iterator());
		reducer.reduce(iterator.getKey(), iterator, reduceCollector, reporter);
	}

	/**
	 * Wrap a hadoop combine() function call and use a Flink collector to collect the result values.
	 * @param values The iterator returning the group of values to be reduced.
	 * @param out The collector to emit the returned values.
	 *
	 * @throws Exception
	 */
	@Override
	public void combine(final Iterable<Tuple2<KEYIN,VALUEIN>> values, final Collector<Tuple2<KEYIN,VALUEIN>> out) throws Exception {
		if (this.combiner == null) {
			throw new RuntimeException("No combiner has been specified in Hadoop job. Flink reduce function is" +
					"declared combinable. Invalid behaviour.");  //This should not happen.
		}
		else {
			combineCollector.setFlinkCollector(out);
			iterator.set(values.iterator());
			combiner.reduce(iterator.getKey(), iterator, combineCollector, reporter);
		}
	}
	
	

	@Override
	public TypeInformation<Tuple2<KEYOUT,VALUEOUT>> getProducedType() {
		final WritableTypeInfo<KEYOUT> keyTypeInfo = new WritableTypeInfo<KEYOUT>(keyOutClass);
		final WritableTypeInfo<VALUEOUT> valueTypleInfo = new WritableTypeInfo<VALUEOUT>(valOutClass);
		return new TupleTypeInfo<Tuple2<KEYOUT,VALUEOUT>>(keyTypeInfo, valueTypleInfo);
	}

	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(final ObjectOutputStream out) throws IOException {
		
		out.writeObject(reducer.getClass());
		out.writeObject(combiner.getClass());
		HadoopConfiguration.writeHadoopJobConf(jobConf, out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
		
		Class<Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>> reducerClass = 
				(Class<Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>>)in.readObject();
		reducer = InstantiationUtil.instantiate(reducerClass);
		
		Class<Reducer<KEYIN,VALUEIN,KEYIN,VALUEIN>> combinerClass = 
				(Class<Reducer<KEYIN,VALUEIN,KEYIN,VALUEIN>>)in.readObject();
		combiner = InstantiationUtil.instantiate(combinerClass);
		
		jobConf = new JobConf();
		jobConf.readFields(in);
	}
}
