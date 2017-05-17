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

package org.apache.flink.streaming.api.functions.windowing.delta;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * This interface allows the implementation of a function which calculates the
 * delta between two data points. Delta functions might be used in delta
 * policies and allow flexible adaptive windowing based on the arriving data
 * points.
 *
 * @param <DATA>
 *            The type of input data which can be compared using this function.
 */
@PublicEvolving
public interface DeltaFunction<DATA> extends Serializable {

	/**
	 * Calculates the delta between two given data points.
	 *
	 * @param oldDataPoint
	 *            the old data point.
	 * @param newDataPoint
	 *            the new data point.
	 * @return the delta between the two given points.
	 */
	double getDelta(DATA oldDataPoint, DATA newDataPoint);

}
