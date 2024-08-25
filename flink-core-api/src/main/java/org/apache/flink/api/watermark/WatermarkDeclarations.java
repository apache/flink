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

package org.apache.flink.api.watermark;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.runtime.watermark.DefaultBoolWatermarkDeclaration;
import org.apache.flink.runtime.watermark.DefaultLongWatermarkDeclaration;

public class WatermarkDeclarations {

    public static WatermarkBuilder newBuilder(String watermarkID) {
        return new WatermarkBuilder(watermarkID);
    }

    /** Builder class for {@link WatermarkDeclaration}s. */
    @Experimental
    public static class WatermarkBuilder {

        private final String watermarkID;

        WatermarkBuilder(String watermarkID) {
            this.watermarkID = watermarkID;
        }

        public LongWatermarkBuilder typeLong() {
            return new LongWatermarkBuilder(watermarkID);
        }

        @Experimental
        public static class LongWatermarkBuilder {
            private WatermarkSpecs.NumericWatermarkComparison watermarkComparison =
                    WatermarkSpecs.NumericWatermarkComparison.MAX;
            private final String watermarkID;

            public LongWatermarkBuilder(String watermarkID) {
                this.watermarkID = watermarkID;
            }

            public LongWatermarkBuilder combinerMax() {
                this.watermarkComparison = WatermarkSpecs.NumericWatermarkComparison.MAX;
                return this;
            }

            public LongWatermarkBuilder combinerMin() {
                this.watermarkComparison = WatermarkSpecs.NumericWatermarkComparison.MIN;
                return this;
            }

            public LongWatermarkDeclaration build() {
                return new DefaultLongWatermarkDeclaration(watermarkComparison, watermarkID);
            }
        }

        @Experimental
        public static class BoolWatermarkBuilder {
            private WatermarkSpecs.BoolWatermarkComparison watermarkComparison =
                    WatermarkSpecs.BoolWatermarkComparison.OR;
            private final String watermarkID;

            public BoolWatermarkBuilder(String watermarkID) {
                this.watermarkID = watermarkID;
            }

            public BoolWatermarkBuilder combinerOR() {
                this.watermarkComparison = WatermarkSpecs.BoolWatermarkComparison.OR;
                return this;
            }

            public BoolWatermarkBuilder combinerAND() {
                this.watermarkComparison = WatermarkSpecs.BoolWatermarkComparison.AND;
                return this;
            }

            public BoolWatermarkDeclaration build() {
                return new DefaultBoolWatermarkDeclaration(watermarkComparison, watermarkID);
            }
        }
    }
}
