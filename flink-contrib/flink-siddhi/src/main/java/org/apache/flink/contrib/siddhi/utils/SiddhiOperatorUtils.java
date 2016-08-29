/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.siddhi.operator.SiddhiStream;
import org.apache.flink.contrib.siddhi.operator.SingleStreamSiddhiOperator;
import org.apache.flink.contrib.siddhi.operator.StreamSiddhiOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Convert SiddhiCEPExecutionPlan to SiddhiCEP Operator and build output DataStream
 */

public class SiddhiOperatorUtils {
	@SuppressWarnings("unchecked")
	public static <OUT> DataStream<OUT> createDataStream(SiddhiStream<OUT> plan){
		if(plan.getInputStreams().size() == 0){
			throw new IllegalArgumentException("Non input streams was connected");
		} else if(plan.getInputStreams().size() == 1){
			Map.Entry<String, DataStream<?>> entry = plan.getInputStreams().entrySet().iterator().next();
			return entry.getValue().transform(plan.getName(),plan.getSiddhiExecutionPlan().getOutputStreamType(),new SingleStreamSiddhiOperator(entry.getKey(),plan.getSiddhiExecutionPlan()));
		} else {
			List<DataStream<Tuple2<String, Object>>> wrappedDataStreams = new ArrayList<>();
			for (Map.Entry<String, DataStream<?>> entry : plan.getInputStreams().entrySet()) {
				wrappedDataStreams.add(wrap(entry.getKey(), entry.getValue()));
			}
			// TODO: Is union correct for our case? Should use broadcast instead.
			DataStream<Tuple2<String, Object>> unionStream = wrappedDataStreams.get(0)
				.union((DataStream<Tuple2<String, Object>>[]) wrappedDataStreams.subList(1,wrappedDataStreams.size()-1).toArray());
			return unionStream.transform(plan.getName(), plan.getSiddhiExecutionPlan().getOutputStreamType(),new StreamSiddhiOperator<Object, OUT>(plan.getSiddhiExecutionPlan()));
		}
	}

	private static <T> DataStream<Tuple2<String,Object>> wrap(final String streamId, DataStream<T> dataStream){
		return dataStream.map(new MapFunction<T, Tuple2<String,Object>>() {
			@Override
			public Tuple2<String, Object> map(T value) throws Exception {
				return Tuple2.of(streamId,(Object) value);
			}
		});
	}
}
