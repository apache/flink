/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.watermark;

import org.apache.flink.api.common.WatermarkDeclaration;
import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class WatermarkUtils {

    public static final WatermarkEvent MAX_TIMESTAMP_WATEMMARK_EVENT =
            new WatermarkEvent(TimestampWatermark.MAX_WATERMARK);

    public static WatermarkEvent createWatermarkEventFromTimestamp(long timestamp) {
        return WatermarkUtils.createWatermarkEventFromTimestamp(timestamp);
    }

    public static Optional<Long> getTimestamp(WatermarkEvent event) {
        Watermark watermark = event.getWatermark();
        if (watermark instanceof TimestampWatermark) {
            return Optional.of(((TimestampWatermark) watermark).getTimestamp());
        } else {
            return Optional.empty();
        }
    }

    public static Optional<Set<WatermarkDeclaration>> getWatermarkCombiner(Object obj) {
        // Check if the object's class is
        // "org.apache.flink.datastream.impl.operators.ProcessOperator"
        Class<?> processOperatorClass;
        try {
            processOperatorClass =
                    Class.forName("org.apache.flink.datastream.impl.operators.ProcessOperator");
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "org.apache.flink.datastream.impl.operators.ProcessOperator class not found",
                    e);
        }

        if (processOperatorClass.isInstance(obj)) {
            try {
                // Access the "watermarkCombiner" method
                Method watermarkCombinerMethod =
                        processOperatorClass.getDeclaredMethod("watermarkDeclarations");
                watermarkCombinerMethod.setAccessible(true);

                // Invoke the method and return the result
                return Optional.ofNullable(
                        (Set<WatermarkDeclaration>) watermarkCombinerMethod.invoke(obj));
            } catch (Exception e) {
                throw new RuntimeException("Failed to get watermark combiner", e);
            }
        } else {
            return Optional.empty();
        }
    }

    public static Set<Class<? extends WatermarkDeclaration>> getWatermarkDeclarations(
            StreamOperator<?> streamOperator) {
        // Check if the object's class is
        // "org.apache.flink.datastream.impl.operators.ProcessOperator"
        Class<?> processOperatorClass;
        try {
            processOperatorClass =
                    Class.forName("org.apache.flink.datastream.impl.operators.ProcessOperator");
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "org.apache.flink.datastream.impl.operators.ProcessOperator class not found",
                    e);
        }

        if (processOperatorClass.isInstance(streamOperator)) {
            try {
                // Access the "watermarkCombiner" method
                Method watermarkCombinerMethod =
                        processOperatorClass.getDeclaredMethod("watermarkDeclarations");
                watermarkCombinerMethod.setAccessible(true);

                // Invoke the method and return the result
                return (Set<Class<? extends WatermarkDeclaration>>)
                        watermarkCombinerMethod.invoke(streamOperator);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get watermark combiner", e);
            }
        } else {
            return Collections.emptySet();
        }
    }
}
