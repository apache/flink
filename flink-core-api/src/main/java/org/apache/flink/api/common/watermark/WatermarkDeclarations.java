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

package org.apache.flink.api.common.watermark;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.watermark.WatermarkCombinationFunction.BoolWatermarkCombinationFunction;
import org.apache.flink.api.common.watermark.WatermarkCombinationFunction.NumericWatermarkCombinationFunction;

/** The Utils class is used to create {@link WatermarkDeclaration}. */
@Experimental
public class WatermarkDeclarations {

    public static WatermarkBuilder newBuilder(String identifier) {
        return new WatermarkBuilder(identifier);
    }

    /** Builder class for {@link WatermarkDeclaration}s. */
    @Experimental
    public static class WatermarkBuilder {

        protected final String identifier;

        WatermarkBuilder(String identifier) {
            this.identifier = identifier;
        }

        public LongWatermarkBuilder typeLong() {
            return new LongWatermarkBuilder(identifier);
        }

        public BoolWatermarkBuilder typeBool() {
            return new BoolWatermarkBuilder(identifier);
        }

        @Experimental
        public static class LongWatermarkBuilder {
            private final String identifier;
            private boolean combineWaitForAllChannels = false;
            // for channels
            private WatermarkCombinationFunction combinationFunction =
                    NumericWatermarkCombinationFunction.MIN;
            // for function
            private WatermarkHandlingStrategy defaultHandlingStrategy =
                    WatermarkHandlingStrategy.FORWARD;

            public LongWatermarkBuilder(String identifier) {
                this.identifier = identifier;
            }

            /** Combine and propagate the maximum watermark to downstream. */
            public LongWatermarkBuilder combineFunctionMax() {
                this.combinationFunction = NumericWatermarkCombinationFunction.MAX;
                return this;
            }

            /** Combine and propagate the minimum watermark to downstream. */
            public LongWatermarkBuilder combineFunctionMin() {
                this.combinationFunction = NumericWatermarkCombinationFunction.MIN;
                return this;
            }

            /**
             * Define whether the framework should send the {@link Watermark} to downstream tasks
             * when the user-defined {@link Watermark} process method returns {@link
             * WatermarkHandlingResult#PEEK}. If set to {@link WatermarkHandlingStrategy#FORWARD},
             * the framework will send the watermark to downstream tasks. If set to {@link
             * WatermarkHandlingStrategy#IGNORE}, the framework will not take any action.
             */
            public LongWatermarkBuilder defaultHandlingStrategy(
                    WatermarkHandlingStrategy strategy) {
                this.defaultHandlingStrategy = strategy;
                return this;
            }

            public LongWatermarkBuilder defaultHandlingStrategyForward() {
                this.defaultHandlingStrategy = WatermarkHandlingStrategy.FORWARD;
                return this;
            }

            public LongWatermarkBuilder defaultHandlingStrategyIgnore() {
                this.defaultHandlingStrategy = WatermarkHandlingStrategy.IGNORE;
                return this;
            }

            /**
             * Whether the combine process should be executed after the process function receives
             * watermarks from both upstream channels.
             */
            public LongWatermarkBuilder combineWaitForAllChannels(
                    boolean combineWaitForAllChannels) {
                this.combineWaitForAllChannels = combineWaitForAllChannels;
                return this;
            }

            public LongWatermarkDeclaration build() {
                return new LongWatermarkDeclaration(
                        identifier,
                        new WatermarkCombinationPolicy(
                                this.combinationFunction, this.combineWaitForAllChannels),
                        this.defaultHandlingStrategy);
            }
        }

        @Experimental
        public static class BoolWatermarkBuilder {
            private final String identifier;
            private boolean combineWaitForAllChannels = false;
            // for channels
            private WatermarkCombinationFunction combinationFunction =
                    BoolWatermarkCombinationFunction.AND;
            // for function
            private WatermarkHandlingStrategy defaultHandlingStrategy =
                    WatermarkHandlingStrategy.FORWARD;

            public BoolWatermarkBuilder(String identifier) {
                this.identifier = identifier;
            }

            /** Propagate the logical OR combination result of boolean watermarks downstream. */
            public BoolWatermarkBuilder combineFunctionOR() {
                this.combinationFunction = BoolWatermarkCombinationFunction.OR;
                return this;
            }

            /** Propagate the logical AND combination result of boolean watermarks downstream. */
            public BoolWatermarkBuilder combineFunctionAND() {
                this.combinationFunction = BoolWatermarkCombinationFunction.AND;
                return this;
            }

            /**
             * Define whether the framework should send the {@link Watermark} to downstream tasks
             * when the user-defined {@link Watermark} process method returns {@link
             * WatermarkHandlingResult#PEEK}. If set to {@link WatermarkHandlingStrategy#FORWARD},
             * the framework will send the watermark to downstream tasks. If set to {@link
             * WatermarkHandlingStrategy#IGNORE}, the framework will not take any action.
             */
            public BoolWatermarkBuilder defaultHandlingStrategy(
                    WatermarkHandlingStrategy strategy) {
                this.defaultHandlingStrategy = strategy;
                return this;
            }

            public BoolWatermarkBuilder defaultHandlingStrategyForward() {
                this.defaultHandlingStrategy = WatermarkHandlingStrategy.FORWARD;
                return this;
            }

            public BoolWatermarkBuilder defaultHandlingStrategyIgnore() {
                this.defaultHandlingStrategy = WatermarkHandlingStrategy.IGNORE;
                return this;
            }

            /**
             * Whether the combine process should be executed after the process function receives
             * watermarks from both upstream channels.
             */
            public BoolWatermarkBuilder combineWaitForAllChannels(
                    boolean combineWaitForAllChannels) {
                this.combineWaitForAllChannels = combineWaitForAllChannels;
                return this;
            }

            public BoolWatermarkDeclaration build() {
                return new BoolWatermarkDeclaration(
                        identifier,
                        new WatermarkCombinationPolicy(
                                this.combinationFunction, this.combineWaitForAllChannels),
                        this.defaultHandlingStrategy);
            }
        }
    }
}
