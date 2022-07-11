package org.apache.flink.connector.base.sink.writer.strategy;

import org.apache.flink.util.Preconditions;

/**
 * A {@code RateLimitingStrategy} implementation that does the following:
 *
 * <ul>
 *   <li>Scales up when any request is successful.
 *   <li>Scales down when any message in a request is unsuccessful.
 *   <li>Uses Additive Increase / Multiplicative Decrease (AIMD) strategy to scale up/down.
 * </ul>
 *
 * <p>This strategy works well for throughput-limited record-based sinks (e.g. Kinesis, Kafka).
 */
public class CongestionControlRateLimitingStrategy implements RateLimitingStrategy {

    private final int maxInFlightRequests;
    private final AIMDScalingStrategy aimdScalingStrategy;
    private int maxInFlightMessages;

    private int currentInFlightRequests;
    private int currentInFlightMessages;

    private CongestionControlRateLimitingStrategy(
            int maxInFlightRequests,
            int initialMaxInFlightMessages,
            AIMDScalingStrategy aimdScalingStrategy) {
        Preconditions.checkArgument(
                maxInFlightRequests > 0, "maxInFlightRequests must be a positive integer.");
        Preconditions.checkArgument(
                initialMaxInFlightMessages > 0,
                "initialMaxInFlightMessages must be a positive integer.");
        Preconditions.checkNotNull(aimdScalingStrategy, "aimdScalingStrategy must be provided.");

        this.maxInFlightRequests = maxInFlightRequests;
        this.maxInFlightMessages = initialMaxInFlightMessages;
        this.aimdScalingStrategy = aimdScalingStrategy;
    }

    @Override
    public void registerInFlightRequest(RequestInfo requestInfo) {
        currentInFlightRequests++;
        currentInFlightMessages += requestInfo.batchSize;
    }

    @Override
    public void registerCompletedRequest(RequestInfo requestInfo) {
        currentInFlightRequests--;
        currentInFlightMessages -= requestInfo.batchSize;
        if (requestInfo.failedMessages > 0) {
            maxInFlightMessages = aimdScalingStrategy.scaleDown(maxInFlightMessages);
        } else {
            maxInFlightMessages = aimdScalingStrategy.scaleUp(maxInFlightMessages);
        }
    }

    @Override
    public boolean shouldBlock(RequestInfo requestInfo) {
        return currentInFlightRequests >= maxInFlightRequests
                || (currentInFlightMessages + requestInfo.batchSize > maxInFlightMessages);
    }

    @Override
    public int getMaxBatchSize() {
        return maxInFlightMessages;
    }

    public static CongestionControlRateLimitingStrategyBuilder builder() {
        return new CongestionControlRateLimitingStrategyBuilder();
    }

    /** Builder for {@link CongestionControlRateLimitingStrategy}. */
    public static class CongestionControlRateLimitingStrategyBuilder {

        private int maxInFlightRequests;
        private int initialMaxInFlightMessages;
        private AIMDScalingStrategy aimdScalingStrategy;

        public CongestionControlRateLimitingStrategyBuilder setMaxInFlightRequests(
                int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public CongestionControlRateLimitingStrategyBuilder setInitialMaxInFlightMessages(
                int initialMaxInFlightMessages) {
            this.initialMaxInFlightMessages = initialMaxInFlightMessages;
            return this;
        }

        public CongestionControlRateLimitingStrategyBuilder setAimdScalingStrategy(
                AIMDScalingStrategy aimdScalingStrategy) {
            this.aimdScalingStrategy = aimdScalingStrategy;
            return this;
        }

        public CongestionControlRateLimitingStrategy build() {
            return new CongestionControlRateLimitingStrategy(
                    maxInFlightRequests, initialMaxInFlightMessages, aimdScalingStrategy);
        }
    }
}
