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

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopTupleUnwrappingIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This wrapper maps a Hadoop Reducer (mapred API) to a non-combinable Flink GroupReduceFunction. 
 */
@SuppressWarnings("rawtypes")
public final class HadoopReduceFunction<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
					extends RichGroupReduceFunction<Tuple2<KEYIN,VALUEIN>,Tuple2<KEYOUT,VALUEOUT>> 
					implements ResultTypeQueryable<Tuple2<KEYOUT,VALUEOUT>>, Serializable {

	private static final long serialVersionUID = 1L;

	private transient Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> reducer;
	private transient JobConf jobConf;
	
	private transient HadoopTupleUnwrappingIterator<KEYIN, VALUEIN> valueIterator;
	private transient HadoopOutputCollector<KEYOUT,VALUEOUT> reduceCollector;
	private transient Reporter reporter;
	
	/**
	 * Maps a Hadoop Reducer (mapred API) to a non-combinable Flink GroupReduceFunction.
 	 * 
	 * @param hadoopReducer The Hadoop Reducer to wrap.
	 */
	public HadoopReduceFunction(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopReducer) {
		this(hadoopReducer, new JobConf());
	}
	
	/**
	 * Maps a Hadoop Reducer (mapred API) to a non-combinable Flink GroupReduceFunction.
 	 * 
	 * @param hadoopReducer The Hadoop Reducer to wrap.
	 * @param conf The JobConf that is used to configure the Hadoop Reducer.
	 */
	public HadoopReduceFunction(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopReducer, JobConf conf) {
		if(hadoopReducer == null) {
			throw new NullPointerException("Reducer may not be null.");
		}
		if(conf == null) {
			throw new NullPointerException("JobConf may not be null.");
		}
		
		this.reducer = hadoopReducer;
		this.jobConf = conf;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.reducer.configure(jobConf);
		
		this.reporter = new HadoopDummyReporter();
		this.reduceCollector = new HadoopOutputCollector<KEYOUT, VALUEOUT>();
		Class<KEYIN> inKeyClass = (Class<KEYIN>) TypeExtractor.getParameterType(Reducer.class, reducer.getClass(), 0);
		this.valueIterator = new HadoopTupleUnwrappingIterator<KEYIN, VALUEIN>(inKeyClass);
	}

	@Override
	public void reduce(final Iterable<Tuple2<KEYIN,VALUEIN>> values, final Collector<Tuple2<KEYOUT,VALUEOUT>> out)
			throws Exception {
		
		reduceCollector.setFlinkCollector(out);
		valueIterator.set(values.iterator());
		reducer.reduce(valueIterator.getCurrentKey(), valueIterator, reduceCollector, reporter);
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeInformation<Tuple2<KEYOUT,VALUEOUT>> getProducedType() {
		Class<KEYOUT> outKeyClass = (Class<KEYOUT>) TypeExtractor.getParameterType(Reducer.class, reducer.getClass(), 2);
		Class<VALUEOUT> outValClass = (Class<VALUEOUT>)TypeExtractor.getParameterType(Reducer.class, reducer.getClass(), 3);

		final TypeInformation<KEYOUT> keyTypeInfo = TypeExtractor.getForClass((Class<KEYOUT>) outKeyClass);
		final TypeInformation<VALUEOUT> valueTypleInfo = TypeExtractor.getForClass((Class<VALUEOUT>) outValClass);
		return new TupleTypeInfo<Tuple2<KEYOUT,VALUEOUT>>(keyTypeInfo, valueTypleInfo);
	}

	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(final ObjectOutputStream out) throws IOException {
		
		out.writeObject(reducer.getClass());
		jobConf.write(out);		
	}

	@SuppressWarnings("unchecked")
	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
		
		Class<Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>> reducerClass = 
				(Class<Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>>)in.readObject();
		reducer = InstantiationUtil.instantiate(reducerClass);
		
		jobConf = new JobConf();
		jobConf.readFields(in);
	}
}
