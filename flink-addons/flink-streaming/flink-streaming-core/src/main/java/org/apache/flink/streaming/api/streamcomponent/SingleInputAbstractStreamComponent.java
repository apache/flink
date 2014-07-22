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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

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
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.invokable.UserSourceInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;

public abstract class SingleInputAbstractStreamComponent<IN extends Tuple, OUT extends Tuple> extends
		AbstractStreamComponent<OUT> {

	protected StreamRecordSerializer<IN> inTupleSerializer = null;

	protected void setSerializers() {
		byte[] operatorBytes = configuration.getBytes("operator", null);
		String operatorName = configuration.getString("operatorName", "");

		Object function = null;
		try {
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(operatorBytes));
			function = in.readObject();

			if (operatorName.equals("flatMap")) {
				setSerializerDeserializer(function, FlatMapFunction.class);
			} else if (operatorName.equals("map")) {
				setSerializerDeserializer(function, MapFunction.class);
			} else if (operatorName.equals("batchReduce")) {
				setSerializerDeserializer(function, GroupReduceFunction.class);
			} else if (operatorName.equals("filter")) {
				setDeserializer(function, FilterFunction.class);
				setSerializer(function, FilterFunction.class, 0);
			} else if (operatorName.equals("sink")) {
				setDeserializer(function, SinkFunction.class);
			} else if (operatorName.equals("source")) {
				setSerializer(function, UserSourceInvokable.class, 0);
			} else if (operatorName.equals("coMap")) {
				setSerializer(function, CoMapFunction.class, 2);
				//setDeserializers(function, CoMapFunction.class);
			} else if (operatorName.equals("elements")) {
				outTupleTypeInfo = new TupleTypeInfo<OUT>(TypeExtractor.getForObject(function));

				outTupleSerializer = new StreamRecordSerializer<OUT>(outTupleTypeInfo.createSerializer());
				outSerializationDelegate = new SerializationDelegate<StreamRecord<OUT>>(
						outTupleSerializer);
			} else {
				throw new Exception("Wrong operator name: " + operatorName);
			}

		} catch (Exception e) {
			throw new StreamComponentException(e);
		}
	}

	private void setSerializerDeserializer(Object function, Class<? extends AbstractFunction> clazz) {
		setDeserializer(function, clazz);
		setSerializer(function, clazz, 1);
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
	protected MutableReader<IOReadableWritable> getConfigInputs() throws StreamComponentException {
		int numberOfInputs = configuration.getInteger("numberOfInputs", 0);

		if (numberOfInputs < 2) {

			return new MutableRecordReader<IOReadableWritable>(this);

		} else {
			MutableRecordReader<IOReadableWritable>[] recordReaders = (MutableRecordReader<IOReadableWritable>[]) new MutableRecordReader<?>[numberOfInputs];

			for (int i = 0; i < numberOfInputs; i++) {
				recordReaders[i] = new MutableRecordReader<IOReadableWritable>(this);
			}
			return new MutableUnionRecordReader<IOReadableWritable>(recordReaders);
		}
	}

}
