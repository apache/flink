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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * The DataStreamSource represents the starting point of a DataStream.
 * 
 * @param <OUT>
 *            Type of the DataStream created.
 */
public class DataStreamSource<OUT> extends SingleOutputStreamOperator<OUT, DataStreamSource<OUT>> {

	boolean isParallel;

	public DataStreamSource(StreamExecutionEnvironment environment, String operatorType,
			TypeInformation<OUT> outTypeInfo, StreamOperator<OUT> operator,
			boolean isParallel, String sourceName) {
		super(environment, operatorType, outTypeInfo, operator);

		environment.getStreamGraph().addSource(getId(), operator, null, outTypeInfo,
				sourceName);

		this.isParallel = isParallel;
		if (!isParallel) {
			setParallelism(1);
		}
	}

	@Override
	public DataStreamSource<OUT> setParallelism(int parallelism) {
		if (parallelism > 1 && !isParallel) {
			throw new IllegalArgumentException("Source: " + this.id + " is not a parallel source");
		} else {
			return (DataStreamSource<OUT>) super.setParallelism(parallelism);
		}
	}
}
