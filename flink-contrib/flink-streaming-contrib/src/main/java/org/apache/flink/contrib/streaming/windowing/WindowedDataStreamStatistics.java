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

package org.apache.flink.contrib.streaming.windowing;

import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.DiscretizedStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.windowing.WindowUtils;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;

/**
 * An extension for {@link WindowedDataStream} with additional statistics
 * functionality. Can be constructed directly through the constructor or
 * through the convenience method {@link DataStreamUtils#statistics(WindowedDataStream)}.
 *
 * @param <OUT>
 *            The output type of the {@link WindowedDataStream}
 */
public class WindowedDataStreamStatistics<OUT> extends WindowedDataStream<OUT> {

	public WindowedDataStreamStatistics(WindowedDataStream<OUT> windowedDataStream) {
		super(windowedDataStream);
	}

	/**
	 * Gives the median of the current window at the specified field at every trigger.
	 * The type of the field can only be Double (as the median of integers might be a fraction).
	 *
	 * The median is updated online as the window changes, and the runtime of
	 * one update is logarithmic with the current window size.
	 *
	 * @param pos
	 *            The position in the tuple/array to calculate the median of
	 * @return The transformed DataStream.
	 */
	@SuppressWarnings("unchecked")
	public DiscretizedStream<OUT> median(int pos) {
		WindowBuffer<OUT> windowBuffer;
		if (groupByKey == null) {
			windowBuffer = new MedianPreReducer<OUT>(pos, getType(), getExecutionConfig());
		} else {
			windowBuffer = new MedianGroupedPreReducer<OUT>(pos, getType(), getExecutionConfig(), groupByKey);
		}
		return discretize(WindowUtils.WindowTransformation.OTHER, windowBuffer);
	}

	/**
	 * Gives the median of the current window at the specified field at every trigger.
	 * The type of the field can only be Double (as the median of integers might be a fraction).
	 *
	 * The field is given by a field expression that is either
	 * the name of a public field or a getter method with parentheses of the
	 * stream's underlying type. A dot can be used to drill down into objects,
	 * as in {@code "field1.getInnerField2()" }.
	 *
	 * The median is updated online as the window changes, and the runtime of
	 * one update is logarithmic with the current window size.
	 *
	 * @param field
	 *            The field to calculate the median of
	 * @return The transformed DataStream.
	 */
	@SuppressWarnings("unchecked")
	public DiscretizedStream<OUT> median(String field) {
		WindowBuffer<OUT> windowBuffer;
		if (groupByKey == null) {
			windowBuffer = new MedianPreReducer<OUT>(field, getType(), getExecutionConfig());
		} else {
			windowBuffer = new MedianGroupedPreReducer<OUT>(field, getType(), getExecutionConfig(), groupByKey);
		}
		return discretize(WindowUtils.WindowTransformation.OTHER, windowBuffer);
	}

}
