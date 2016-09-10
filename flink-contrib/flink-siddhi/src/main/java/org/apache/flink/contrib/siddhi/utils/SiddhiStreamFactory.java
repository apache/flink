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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.siddhi.operator.SiddhiOperatorContext;
import org.apache.flink.contrib.siddhi.operator.TupleStreamSiddhiOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Convert SiddhiCEPExecutionPlan to SiddhiCEP Operator and build output DataStream
 */
public class SiddhiStreamFactory {
	@SuppressWarnings("unchecked")
	public static <OUT> DataStream<OUT> createDataStream(SiddhiOperatorContext context, DataStream<Tuple2<String, Object>> namedStream) {
		return namedStream.transform(context.getName(), context.getOutputStreamType(), new TupleStreamSiddhiOperator(context));
	}
}
