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

package org.apache.flink.streaming.api.watermark;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermarks;
import org.apache.flink.watermark.DefaultLongWatermarkDeclaration;

/** Builder class for {@link WatermarkDeclaration}s. */
@Experimental
public class WatermarkBuilder {

    public static LongWatermarkBuilder withLongWatermark() {
        return new LongWatermarkBuilder();
    }

    @Experimental
    public static class LongWatermarkBuilder {
        private Watermarks.NumericWatermarkComparison watermarkComparison =
                Watermarks.NumericWatermarkComparison.MAX;

        public LongWatermarkBuilder withCompareSemantics(
                Watermarks.NumericWatermarkComparison watermarkComparison) {
            this.watermarkComparison = watermarkComparison;
            return this;
        }

        public LongWatermarkDeclaration build() {
            return new DefaultLongWatermarkDeclaration(watermarkComparison);
        }
    }

    //
    //
    //
    //    public static BoolWatermarkBuilder withBoolWatermark() {
    //        return new BoolWatermarkBuilder();
    //    }
    //
    //    @Experimental
    //    public static class BoolWatermarkBuilder {
    //        private Watermarks.BoolWatermarkComparison watermarkComparison =
    //                Watermarks.BoolWatermarkComparison.AND;
    //
    //        public BoolWatermarkBuilder withCompareSemantics(
    //                Watermarks.BoolWatermarkComparison watermarkComparison) {
    //            this.watermarkComparison = watermarkComparison;
    //            return this;
    //        }
    //
    //        public BoolWatermarkDeclaration build() {
    //            return new DefaultBoolWatermarkDeclaration(watermarkComparison);
    //        }
    //    }
    //
    //
    //
    //    public static RecordAtrributesWatermarkBuilder withRecordAttributesWatermark() {
    //        return new RecordAtrributesWatermarkBuilder();
    //    }
    //
    //    @Experimental
    //    public static class RecordAtrributesWatermarkBuilder {
    //        private Watermarks.BoolWatermarkComparison watermarkComparison =
    //                Watermarks.BoolWatermarkComparison.AND;
    //
    //        public RecordAtrributesWatermarkBuilder withCompareSemantics(
    //                Watermarks.BoolWatermarkComparison watermarkComparison) {
    //            this.watermarkComparison = watermarkComparison;
    //            return this;
    //        }
    //
    //        public RecordAtrributesWatermarkDeclaration build() {
    //            return new DefaultRecordAtrributesWatermarkDeclaration(watermarkComparison);
    //        }
    //    }
}
