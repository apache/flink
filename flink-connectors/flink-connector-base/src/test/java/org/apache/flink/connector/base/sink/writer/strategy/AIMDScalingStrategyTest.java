package org.apache.flink.connector.base.sink.writer.strategy;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test class for {@link AIMDScalingStrategy}. */
public class AIMDScalingStrategyTest {

    @Test
    public void testScaleUpAdditively() {
        final int increaseRate = 10;
        final int currentRate = 4;

        AIMDScalingStrategy scalingStrategy =
                AIMDScalingStrategy.builder()
                        .setIncreaseRate(increaseRate)
                        .setDecreaseFactor(0.5)
                        .setRateThreshold(1000)
                        .build();

        assertThat(scalingStrategy.scaleUp(currentRate)).isEqualTo(increaseRate + currentRate);
    }

    @Test
    public void testScaleUpRespectsRateThreshold() {
        final int increaseRate = 10;
        final int currentRate = 14;
        final int rateThreshold = 20;

        AIMDScalingStrategy scalingStrategy =
                AIMDScalingStrategy.builder()
                        .setIncreaseRate(increaseRate)
                        .setDecreaseFactor(0.5)
                        .setRateThreshold(rateThreshold)
                        .build();

        assertThat(scalingStrategy.scaleUp(increaseRate + currentRate)).isEqualTo(rateThreshold);
    }

    @Test
    public void testScaleDownByFactor() {
        final double decreaseFactor = 0.4;

        AIMDScalingStrategy scalingStrategy =
                AIMDScalingStrategy.builder()
                        .setIncreaseRate(10)
                        .setDecreaseFactor(decreaseFactor)
                        .setRateThreshold(1000)
                        .build();

        // roundDown(10 * 0.4 = 4) = 4
        assertThat(scalingStrategy.scaleDown(10)).isEqualTo(4);

        // roundDown(314 * 0.4 = 125.6) = 125
        assertThat(scalingStrategy.scaleDown(314)).isEqualTo(126);
    }

    @Test
    public void testScaleDownByFactorStopsAtOne() {
        final double decreaseFactor = 0.1;

        AIMDScalingStrategy scalingStrategy =
                AIMDScalingStrategy.builder()
                        .setIncreaseRate(10)
                        .setDecreaseFactor(decreaseFactor)
                        .setRateThreshold(1000)
                        .build();

        // even though roundDown(1 * 0.1 = 0.1) = 0 , the lowest we go is 1
        assertThat(scalingStrategy.scaleDown(1)).isEqualTo(1);
    }

    @Test
    public void testInvalidIncreaseRate() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                AIMDScalingStrategy.builder()
                                        .setIncreaseRate(0)
                                        .setDecreaseFactor(0.5)
                                        .setRateThreshold(100)
                                        .build())
                .withMessageContaining("increaseRate must be positive integer.");
    }

    @Test
    public void testInvalidDecreaseFactor() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                AIMDScalingStrategy.builder()
                                        .setIncreaseRate(1)
                                        .setDecreaseFactor(0)
                                        .setRateThreshold(100)
                                        .build())
                .withMessageContaining("decreaseFactor must be strictly between 0.0 and 1.0.");

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                AIMDScalingStrategy.builder()
                                        .setIncreaseRate(1)
                                        .setDecreaseFactor(1.0)
                                        .setRateThreshold(100)
                                        .build())
                .withMessageContaining("decreaseFactor must be strictly between 0.0 and 1.0.");
    }

    @Test
    public void testInvalidRateThreshold() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                AIMDScalingStrategy.builder()
                                        .setIncreaseRate(1)
                                        .setDecreaseFactor(0.5)
                                        .setRateThreshold(0)
                                        .build())
                .withMessageContaining("rateThreshold must be a positive integer.");
    }

    @Test
    public void testIncreaseRateLargerThanRateThresholdRejected() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                AIMDScalingStrategy.builder()
                                        .setIncreaseRate(10)
                                        .setDecreaseFactor(0.5)
                                        .setRateThreshold(5)
                                        .build())
                .withMessageContaining("rateThreshold must be larger than increaseRate.");
    }
}
