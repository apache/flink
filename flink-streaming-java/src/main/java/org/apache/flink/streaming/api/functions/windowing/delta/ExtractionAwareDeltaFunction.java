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
 * Extend this abstract class to implement a delta function which is aware of extracting the data on
 * which the delta is calculated from a more complex data structure. For example in case you want to
 * be able to run a delta only on one field of a Tuple type or only on some fields from an array.
 *
 * @param <DATA> The input data type. The input of this type will be passed to the extractor which
 *     will transform into a TO-object. The delta function then runs on this TO-object.
 * @param <TO> The type on which the delta function runs. (The type of the delta function)
 */
@PublicEvolving
public abstract class ExtractionAwareDeltaFunction<DATA, TO> implements DeltaFunction<DATA> {

    private static final long serialVersionUID = 6927486219702689554L;

    private Extractor<DATA, TO> converter;

    public ExtractionAwareDeltaFunction(Extractor<DATA, TO> converter) {
        this.converter = converter;
    }

    /**
     * This method takes the two data point and runs the set extractor on it. The delta function
     * implemented at {@link #getNestedDelta} is then called with the extracted data. In case no
     * extractor is set the input data gets passes to {@link #getNestedDelta} as-is. The return
     * value is just forwarded from {@link #getNestedDelta}.
     *
     * @param oldDataPoint the older data point as raw data (before extraction).
     * @param newDataPoint the new data point as raw data (before extraction).
     * @return the delta between the two points.
     */
    @SuppressWarnings("unchecked")
    @Override
    public double getDelta(DATA oldDataPoint, DATA newDataPoint) {
        if (converter == null) {
            // In case no conversion/extraction is required, we can cast DATA to
            // TO
            // => Therefore, "unchecked" warning is suppressed for this method.
            return getNestedDelta((TO) oldDataPoint, (TO) newDataPoint);
        } else {
            return getNestedDelta(converter.extract(oldDataPoint), converter.extract(newDataPoint));
        }
    }

    /**
     * This method is exactly the same as {@link DeltaFunction#getDelta(Object, Object)} except that
     * it gets the result of the previously done extractions as input. Therefore, this method only
     * does the actual calculation of the delta but no data extraction or conversion.
     *
     * @param oldDataPoint the older data point.
     * @param newDataPoint the new data point.
     * @return the delta between the two points.
     */
    public abstract double getNestedDelta(TO oldDataPoint, TO newDataPoint);
}
