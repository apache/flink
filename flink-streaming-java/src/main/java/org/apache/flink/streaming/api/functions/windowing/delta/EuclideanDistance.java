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
import org.apache.flink.streaming.api.functions.windowing.delta.extractor.Extractor;

/**
 * This delta function calculates the euclidean distance between two given
 * points.
 *
 * <p>Euclidean distance: http://en.wikipedia.org/wiki/Euclidean_distance
 *
 * @param <DATA>
 *            The input data type. This delta function works with a double[],
 *            but can extract/convert to it from any other given object in case
 *            the respective extractor has been set. See
 *            {@link ExtractionAwareDeltaFunction} for more information.
 */
@PublicEvolving
public class EuclideanDistance<DATA> extends ExtractionAwareDeltaFunction<DATA, double[]> {

	public EuclideanDistance() {
		super(null);
	}

	public EuclideanDistance(Extractor<DATA, double[]> converter) {
		super(converter);
	}

	private static final long serialVersionUID = 3119432599634512359L;

	@Override
	public double getNestedDelta(double[] oldDataPoint, double[] newDataPoint) {
		double result = 0;
		for (int i = 0; i < oldDataPoint.length; i++) {
			result += (oldDataPoint[i] - newDataPoint[i]) * (oldDataPoint[i] - newDataPoint[i]);
		}
		return Math.sqrt(result);
	}

}
