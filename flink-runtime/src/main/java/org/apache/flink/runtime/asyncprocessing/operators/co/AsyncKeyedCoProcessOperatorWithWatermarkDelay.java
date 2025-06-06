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

package org.apache.flink.runtime.asyncprocessing.operators.co;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;

/** A {@link KeyedCoProcessOperator} that supports holding back watermarks with a static delay. */
public class AsyncKeyedCoProcessOperatorWithWatermarkDelay<K, IN1, IN2, OUT>
        extends AsyncKeyedCoProcessOperator<K, IN1, IN2, OUT> {
    private static final long serialVersionUID = 1L;

    private final long watermarkDelay;

    public AsyncKeyedCoProcessOperatorWithWatermarkDelay(
            KeyedCoProcessFunction<K, IN1, IN2, OUT> keyedCoProcessFunction, long watermarkDelay) {
        super(keyedCoProcessFunction);
        Preconditions.checkArgument(
                watermarkDelay >= 0, "The watermark delay should be non-negative.");
        this.watermarkDelay = watermarkDelay;
    }

    @Override
    public Watermark postProcessWatermark(Watermark watermark) throws Exception {
        if (watermarkDelay == 0) {
            return watermark;
        } else {
            return new Watermark(watermark.getTimestamp() - watermarkDelay);
        }
    }
}
