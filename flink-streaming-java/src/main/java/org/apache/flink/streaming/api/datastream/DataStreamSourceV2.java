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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSourceV2;
import org.apache.flink.streaming.api.transformations.SourceV2Transformation;

/**
 * The DataStreamSourceV2 represents the starting point of a DataStream using V2 interface.
 *
 * @param <T> Type of the elements in the DataStream created from the this source.
 */
@Public
public class DataStreamSourceV2<T> extends SingleOutputStreamOperator<T> {

	boolean isParallel;

	public DataStreamSourceV2(StreamExecutionEnvironment environment,
			TypeInformation<T> outTypeInfo, StreamSourceV2<T, ?> operator,
			boolean isParallel, String sourceName) {
		super(environment, new SourceV2Transformation<>(sourceName, operator, outTypeInfo, environment.getParallelism()));

		this.isParallel = isParallel;
		if (!isParallel) {
			setMaxParallelism(1);
			setParallelism(1);
		}
	}

	public DataStreamSourceV2(SingleOutputStreamOperator<T> operator) {
		super(operator.environment, operator.getTransformation());
		this.isParallel = true;
	}

	@Override
	public DataStreamSourceV2<T> setParallelism(int parallelism) {
		if (parallelism != 1 && !isParallel) {
			throw new IllegalArgumentException("Source: " + transformation.getId() + " is not a parallel source");
		} else {
			super.setParallelism(parallelism);
			return this;
		}
	}
}
