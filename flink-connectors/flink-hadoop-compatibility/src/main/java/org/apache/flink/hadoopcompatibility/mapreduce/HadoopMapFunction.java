/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.hadoopcompatibility.mapreduce;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.mapreduce.wrapper.HadoopProxyMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * This wrapper maps a Hadoop Mapper (mapreduce API) to a Flink FlatMapFunction.
 */
@SuppressWarnings("rawtypes")
@Public
public class HadoopMapFunction<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	extends RichFlatMapFunction<Tuple2<KEYIN, VALUEIN>, Tuple2<KEYOUT, VALUEOUT>>
	implements ResultTypeQueryable<Tuple2<KEYOUT, VALUEOUT>>, Serializable {

	private static final long serialVersionUID = 1L;

	private transient HadoopProxyMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopProxyMapper;
	private transient Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopMapper;
	private transient Job jobConf;
	private transient HadoopProxyMapper.HadoopDummyMapperContext mapperContext;

	public HadoopMapFunction(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopMapper) throws IOException {
		this(hadoopMapper, Job.getInstance());
	}

	public HadoopMapFunction(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopMapper, Job conf) {
		this.hadoopMapper = Preconditions.checkNotNull(hadoopMapper);
		this.hadoopProxyMapper = new HadoopProxyMapper<>();
		this.hadoopProxyMapper.setDelegatedMapper(hadoopMapper);
		this.jobConf = Preconditions.checkNotNull(conf);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.hadoopProxyMapper.init();
		this.mapperContext = this.hadoopProxyMapper.new HadoopDummyMapperContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>();
		this.mapperContext.setHadoopConf(jobConf.getConfiguration());
		this.hadoopProxyMapper.setup(this.mapperContext);
	}

	@Override
	public void flatMap(Tuple2<KEYIN, VALUEIN> value, Collector<Tuple2<KEYOUT, VALUEOUT>> out) throws Exception {
		mapperContext.setFlinkCollector(out);
		hadoopProxyMapper.map(value.f0, value.f1, mapperContext);
	}

	@Override
	public void close() throws Exception {
		super.close();
		this.hadoopProxyMapper.cleanup(this.mapperContext);
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeInformation<Tuple2<KEYOUT, VALUEOUT>> getProducedType() {
		Class<KEYOUT> outKeyClass = (Class<KEYOUT>) TypeExtractor.getParameterType(org.apache.hadoop.mapreduce.Mapper.class, hadoopProxyMapper.getDelegatedMapper().getClass(), 2);
		Class<VALUEOUT> outValClass = (Class<VALUEOUT>) TypeExtractor.getParameterType(org.apache.hadoop.mapreduce.Mapper.class, hadoopProxyMapper.getDelegatedMapper().getClass(), 3);

		final TypeInformation<KEYOUT> keyTypeInfo = TypeExtractor.getForClass((Class<KEYOUT>) outKeyClass);
		final TypeInformation<VALUEOUT> valueTypleInfo = TypeExtractor.getForClass((Class<VALUEOUT>) outValClass);
		return new TupleTypeInfo<Tuple2<KEYOUT, VALUEOUT>>(keyTypeInfo, valueTypleInfo);
	}

	/**
	 * Custom serialization methods.
	 * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html">http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html</a>
	 */
	private void writeObject(final ObjectOutputStream out) throws IOException {
		out.writeObject(hadoopProxyMapper.getClass());
		out.writeObject(hadoopMapper.getClass());
		jobConf.getConfiguration().write(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
		Class<HadoopProxyMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> dummyMapperClass =
			(Class<HadoopProxyMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>) in.readObject();
		Class<Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> mapperClass =
			(Class<Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>) in.readObject();
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		configuration.readFields(in);

		hadoopProxyMapper = InstantiationUtil.instantiate(dummyMapperClass);
		hadoopMapper = InstantiationUtil.instantiate(mapperClass);
		hadoopProxyMapper.setDelegatedMapper(hadoopMapper);

		jobConf = Job.getInstance(configuration);
	}
}
