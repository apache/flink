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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.watermark.WatermarkDeclaration;
import org.apache.flink.datastream.watermark.DeclarableWatermark;
import org.apache.flink.runtime.watermark.InternalBoolWatermarkDeclaration;
import org.apache.flink.runtime.watermark.InternalLongWatermarkDeclaration;
import org.apache.flink.runtime.watermark.InternalWatermarkDeclaration;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;

import java.util.Collection;
import java.util.Collections;

public class WatermarkUtils {

    public static Collection<? extends WatermarkDeclaration> getWatermarkDeclarations(
            StreamOperator<?> streamOperator) {
        if (streamOperator instanceof AbstractUdfStreamOperator) {
            Function f = ((AbstractUdfStreamOperator<?, ?>) streamOperator).getUserFunction();
            if (f instanceof DeclarableWatermark) {
                return ((DeclarableWatermark) f).watermarkDeclarations();
            }
        }
        return Collections.emptySet();
    }

    public static InternalWatermarkDeclaration convertToInternalWatermarkDeclaration(
            WatermarkDeclaration watermarkDeclaration) {
        if (watermarkDeclaration instanceof LongWatermarkDeclaration) {
            return new InternalLongWatermarkDeclaration(
                    watermarkDeclaration.getIdentifier(),
                    ((LongWatermarkDeclaration) watermarkDeclaration).getComparisonSemantics());
        } else if (watermarkDeclaration instanceof BoolWatermarkDeclaration) {
            return new InternalBoolWatermarkDeclaration(
                    watermarkDeclaration.getIdentifier(),
                    ((BoolWatermarkDeclaration) watermarkDeclaration).getComparisonSemantics());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported watermark declaration: " + watermarkDeclaration);
        }
    }
}
