package org.apache.flink.connector.base.sink.writer.strategy;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test class for {@link CongestionControlRateLimitingStrategy}. */
public class CongestionControlRateLimitingStrategyTest {
    @Test
    public void testMaxInFlightRequestsRespected() {
        final int maxInFlightRequests = 2;
        CongestionControlRateLimitingStrategy strategy =
                CongestionControlRateLimitingStrategy.builder()
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setInitialMaxInFlightMessages(10)
                        .setAimdScalingStrategy(
                                AIMDScalingStrategy.builder().setRateThreshold(10).build())
                        .build();

        final RequestInfo emptyRequest = RequestInfo.builder().build();

        for (int i = 0; i < maxInFlightRequests; i++) {
            assertThat(strategy.shouldBlock(emptyRequest)).isFalse();
            strategy.registerInFlightRequest(emptyRequest);
        }
        assertThat(strategy.shouldBlock(emptyRequest)).isTrue();

        strategy.registerCompletedRequest(emptyRequest);
        assertThat(strategy.shouldBlock(emptyRequest)).isFalse();
    }

    @Test
    public void testInitialMaxInFlightMessagesRespected() {
        final RequestInfo requestWithTwoMessages = RequestInfo.builder().setBatchSize(2).build();

        CongestionControlRateLimitingStrategy strategy =
                CongestionControlRateLimitingStrategy.builder()
                        .setMaxInFlightRequests(100)
                        .setInitialMaxInFlightMessages(4)
                        .setAimdScalingStrategy(
                                AIMDScalingStrategy.builder()
                                        .setIncreaseRate(1)
                                        .setRateThreshold(4)
                                        .build())
                        .build();

        strategy.registerInFlightRequest(requestWithTwoMessages);
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isFalse();

        strategy.registerInFlightRequest(requestWithTwoMessages);
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isTrue();

        strategy.registerCompletedRequest(requestWithTwoMessages);
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isFalse();
    }

    @Test
    public void testAimdScalingStrategyScaleUpOnSuccess() {
        final RequestInfo emptySuccessfulRequest =
                RequestInfo.builder().setFailedMessages(0).build();
        final RequestInfo requestWithTwoMessages =
                RequestInfo.builder().setFailedMessages(0).setBatchSize(2).build();
        AIMDScalingStrategy aimdScalingStrategy =
                AIMDScalingStrategy.builder()
                        .setIncreaseRate(10)
                        .setRateThreshold(100)
                        .setDecreaseFactor(0.5)
                        .build();

        CongestionControlRateLimitingStrategy strategy =
                CongestionControlRateLimitingStrategy.builder()
                        .setMaxInFlightRequests(100)
                        .setInitialMaxInFlightMessages(1)
                        .setAimdScalingStrategy(aimdScalingStrategy)
                        .build();

        assertThat(strategy.shouldBlock(emptySuccessfulRequest)).isFalse();
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isTrue();

        strategy.registerInFlightRequest(emptySuccessfulRequest);
        strategy.registerCompletedRequest(emptySuccessfulRequest);

        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isFalse();
    }

    @Test
    public void testAimdScalingStrategyScaleDownOnFailure() {
        final RequestInfo emptyFailedRequest = RequestInfo.builder().setFailedMessages(1).build();
        final RequestInfo requestWithTwoMessages =
                RequestInfo.builder().setFailedMessages(0).setBatchSize(2).build();
        AIMDScalingStrategy aimdScalingStrategy =
                AIMDScalingStrategy.builder().setDecreaseFactor(0.5).setRateThreshold(100).build();

        CongestionControlRateLimitingStrategy strategy =
                CongestionControlRateLimitingStrategy.builder()
                        .setMaxInFlightRequests(100)
                        .setInitialMaxInFlightMessages(2)
                        .setAimdScalingStrategy(aimdScalingStrategy)
                        .build();

        assertThat(strategy.shouldBlock(emptyFailedRequest)).isFalse();
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isFalse();

        strategy.registerInFlightRequest(emptyFailedRequest);
        strategy.registerCompletedRequest(emptyFailedRequest);

        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isTrue();
    }

    @Test
    public void testInvalidMaxInFlightRequests() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                CongestionControlRateLimitingStrategy.builder()
                                        .setMaxInFlightRequests(0)
                                        .setInitialMaxInFlightMessages(10)
                                        .setAimdScalingStrategy(
                                                AIMDScalingStrategy.builder()
                                                        .setRateThreshold(10)
                                                        .build())
                                        .build())
                .withMessageContaining("maxInFlightRequests must be a positive integer.");
    }

    @Test
    public void testInvalidMaxInFlightMessages() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                CongestionControlRateLimitingStrategy.builder()
                                        .setMaxInFlightRequests(10)
                                        .setInitialMaxInFlightMessages(0)
                                        .setAimdScalingStrategy(
                                                AIMDScalingStrategy.builder()
                                                        .setRateThreshold(10)
                                                        .build())
                                        .build())
                .withMessageContaining("initialMaxInFlightMessages must be a positive integer.");
    }

    @Test
    public void testInvalidAimdStrategy() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                CongestionControlRateLimitingStrategy.builder()
                                        .setMaxInFlightRequests(10)
                                        .setInitialMaxInFlightMessages(10)
                                        .build())
                .withMessageContaining("aimdScalingStrategy must be provided.");
    }
}
