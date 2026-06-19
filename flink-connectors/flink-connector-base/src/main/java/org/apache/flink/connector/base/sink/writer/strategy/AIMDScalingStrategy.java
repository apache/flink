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

package org.apache.flink.connector.base.sink.writer.strategy;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

/**
 * AIMDScalingStrategy scales up linearly and scales down multiplicatively. See
 * https://en.wikipedia.org/wiki/Additive_increase/multiplicative_decrease for more details
 */
@PublicEvolving
public class AIMDScalingStrategy implements ScalingStrategy<Integer> {
    private final int increaseRate;
    private final double decreaseFactor;
    private final int rateThreshold;

    public AIMDScalingStrategy(int increaseRate, double decreaseFactor, int rateThreshold) {
        Preconditions.checkArgument(increaseRate > 0, "increaseRate must be positive integer.");
        Preconditions.checkArgument(
                decreaseFactor < 1.0 && decreaseFactor > 0.0,
                "decreaseFactor must be strictly between 0.0 and 1.0.");
        Preconditions.checkArgument(rateThreshold > 0, "rateThreshold must be a positive integer.");
        this.increaseRate = increaseRate;
        this.decreaseFactor = decreaseFactor;
        this.rateThreshold = rateThreshold;
    }

    @Override
    public Integer scaleUp(Integer currentRate) {
        return Math.min(currentRate + increaseRate, rateThreshold);
    }

    @Override
    public Integer scaleDown(Integer currentRate) {
        return Math.max(1, (int) Math.round(currentRate * decreaseFactor));
    }

    @PublicEvolving
    public static AIMDScalingStrategyBuilder builder(int rateThreshold) {
        return new AIMDScalingStrategyBuilder(rateThreshold);
    }

    /** Builder for {@link AIMDScalingStrategy}. */
    @PublicEvolving
    public static class AIMDScalingStrategyBuilder {

        private final int rateThreshold;
        private int increaseRate = 10;
        private double decreaseFactor = 0.5;

        public AIMDScalingStrategyBuilder(int rateThreshold) {
            this.rateThreshold = rateThreshold;
        }

        public AIMDScalingStrategyBuilder setIncreaseRate(int increaseRate) {
            this.increaseRate = increaseRate;
            return this;
        }

        public AIMDScalingStrategyBuilder setDecreaseFactor(double decreaseFactor) {
            this.decreaseFactor = decreaseFactor;
            return this;
        }

        public AIMDScalingStrategy build() {
            return new AIMDScalingStrategy(increaseRate, decreaseFactor, rateThreshold);
        }
    }
}
