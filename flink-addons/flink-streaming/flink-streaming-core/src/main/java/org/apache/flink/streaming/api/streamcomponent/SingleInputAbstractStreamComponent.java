/**
 *
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
 *
 */

package org.apache.flink.streaming.api.streamcomponent;

import org.apache.flink.api.common.functions.AbstractFunction;
import org.apache.flink.api.java.functions.FilterFunction;
import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.MutableUnionRecordReader;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.invokable.UserSourceInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.MutableObjectIterator;

public abstract class SingleInputAbstractStreamComponent<IN extends Tuple, OUT extends Tuple> extends
		AbstractStreamComponent<OUT> {

	protected StreamRecordSerializer<IN> inTupleSerializer = null;
	protected MutableObjectIterator<StreamRecord<IN>> inputIter;
	protected MutableReader<IOReadableWritable> inputs;

	protected void setDeserializers() {
		try {
			if (functionName.equals("flatMap")) {
				setDeserializer(function, FlatMapFunction.class);
			} else if (functionName.equals("map")) {
				setDeserializer(function, MapFunction.class);
			} else if (functionName.equals("batchReduce")) {
				setDeserializer(function, GroupReduceFunction.class);
			} else if (functionName.equals("filter")) {
				setDeserializer(function, FilterFunction.class);
			} else if (functionName.equals("source")) {
				setSerializer(function, UserSourceInvokable.class, 0);
			} else if (functionName.equals("sink")) {
				setDeserializer(function, SinkFunction.class);
			} else {
				throw new Exception("Wrong operator name: " + functionName);
			}

		} catch (Exception e) {
			throw new StreamComponentException(e);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void setDeserializer(Object function, Class<? extends AbstractFunction> clazz) {
		TupleTypeInfo<IN> inTupleTypeInfo = (TupleTypeInfo) TypeExtractor.createTypeInfo(clazz, function.getClass(),
				0, null, null);

		inTupleSerializer = new StreamRecordSerializer(inTupleTypeInfo.createSerializer());
	}
	
	@SuppressWarnings("unchecked")
	protected void setSinkSerializer() {
		if (outSerializationDelegate != null) {
			TupleTypeInfo<IN> inTupleTypeInfo = (TupleTypeInfo<IN>) outTupleTypeInfo;

			inTupleSerializer = new StreamRecordSerializer<IN>(inTupleTypeInfo.createSerializer());
		}
	}

	@SuppressWarnings("unchecked")
	protected void setConfigInputs() throws StreamComponentException {
		setDeserializers();
		
		int numberOfInputs = configuration.getNumberOfInputs();

		if (numberOfInputs < 2) {

			inputs = new MutableRecordReader<IOReadableWritable>(this);

		} else {
			MutableRecordReader<IOReadableWritable>[] recordReaders = (MutableRecordReader<IOReadableWritable>[]) new MutableRecordReader<?>[numberOfInputs];

			for (int i = 0; i < numberOfInputs; i++) {
				recordReaders[i] = new MutableRecordReader<IOReadableWritable>(this);
			}
			inputs = new MutableUnionRecordReader<IOReadableWritable>(recordReaders);
		}
	}

}
