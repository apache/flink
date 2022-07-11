package org.apache.flink.connector.base.sink.writer.strategy;

import org.apache.flink.util.Preconditions;

/**
 * AIMDScalingStrategy scales up linearly and scales down multiplicatively. See
 * https://en.wikipedia.org/wiki/Additive_increase/multiplicative_decrease for more details
 */
public class AIMDScalingStrategy {
    private final int increaseRate;
    private final double decreaseFactor;
    private final int rateThreshold;

    public AIMDScalingStrategy(int increaseRate, double decreaseFactor, int rateThreshold) {
        Preconditions.checkArgument(increaseRate > 0, "increaseRate must be positive integer.");
        Preconditions.checkArgument(
                decreaseFactor < 1.0 && decreaseFactor > 0.0,
                "decreaseFactor must be strictly between 0.0 and 1.0.");
        Preconditions.checkArgument(rateThreshold > 0, "rateThreshold must be a positive integer.");
        Preconditions.checkArgument(
                rateThreshold >= increaseRate, "rateThreshold must be larger than increaseRate.");
        this.increaseRate = increaseRate;
        this.decreaseFactor = decreaseFactor;
        this.rateThreshold = rateThreshold;
    }

    public int scaleUp(int currentRate) {
        return Math.min(currentRate + increaseRate, rateThreshold);
    }

    public int scaleDown(int currentRate) {
        return Math.max(1, (int) Math.round(currentRate * decreaseFactor));
    }

    public static AIMDScalingStrategyBuilder builder() {
        return new AIMDScalingStrategyBuilder();
    }

    /** Builder for {@link AIMDScalingStrategy}. */
    public static class AIMDScalingStrategyBuilder {

        private int increaseRate = 10;
        private double decreaseFactor = 0.5;
        private int rateThreshold;

        public AIMDScalingStrategyBuilder setIncreaseRate(int increaseRate) {
            this.increaseRate = increaseRate;
            return this;
        }

        public AIMDScalingStrategyBuilder setDecreaseFactor(double decreaseFactor) {
            this.decreaseFactor = decreaseFactor;
            return this;
        }

        public AIMDScalingStrategyBuilder setRateThreshold(int rateThreshold) {
            this.rateThreshold = rateThreshold;
            return this;
        }

        public AIMDScalingStrategy build() {
            return new AIMDScalingStrategy(increaseRate, decreaseFactor, rateThreshold);
        }
    }
}
