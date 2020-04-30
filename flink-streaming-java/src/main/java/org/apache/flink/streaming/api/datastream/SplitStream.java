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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.transformations.SelectTransformation;
import org.apache.flink.streaming.api.transformations.SplitTransformation;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

/**
 * The SplitStream represents an operator that has been split using an
 * {@link OutputSelector}. Named outputs can be selected using the
 * {@link #select} function. To apply transformation on the whole output simply
 * call the transformation on the SplitStream
 *
 * @param <OUT> The type of the elements in the Stream
 */

@Deprecated
@PublicEvolving
public class SplitStream<OUT> extends DataStream<OUT> {

	protected SplitStream(DataStream<OUT> dataStream, OutputSelector<OUT> outputSelector) {
		super(dataStream.getExecutionEnvironment(), new SplitTransformation<OUT>(dataStream.getTransformation(), outputSelector));
	}

	/**
	 * Sets the output names for which the next operator will receive values.
	 *
	 * @param outputNames
	 *            The output names for which the operator will receive the
	 *            input.
	 * @return Returns the selected DataStream
	 */
	public DataStream<OUT> select(String... outputNames) {
		return selectOutput(outputNames);
	}

	private DataStream<OUT> selectOutput(String[] outputNames) {
		for (String outName : outputNames) {
			if (outName == null) {
				throw new RuntimeException("Selected names must not be null");
			}
		}

		SelectTransformation<OUT> selectTransform = new SelectTransformation<OUT>(this.getTransformation(), Lists.newArrayList(outputNames));
		return new DataStream<OUT>(this.getExecutionEnvironment(), selectTransform);
	}

}
