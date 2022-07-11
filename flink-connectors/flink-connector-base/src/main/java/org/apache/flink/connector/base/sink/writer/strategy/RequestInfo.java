package org.apache.flink.connector.base.sink.writer.strategy;

/** Dataclass to encapsulate information about starting / completing requests. */
public class RequestInfo {
    public final int failedMessages;
    public int batchSize;
    public final long requestStartTime;

    private RequestInfo(int failedMessages, int batchSize, long requestStartTime) {
        this.failedMessages = failedMessages;
        this.batchSize = batchSize;
        this.requestStartTime = requestStartTime;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public static RequestInfoBuilder builder() {
        return new RequestInfoBuilder();
    }

    /** Builder for {@link RequestInfo} dataclass. */
    public static class RequestInfoBuilder {
        private int failedMessages;
        private int batchSize;
        private long requestStartTime;

        public RequestInfoBuilder setBatchSize(final int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public RequestInfoBuilder setFailedMessages(final int failedMessages) {
            this.failedMessages = failedMessages;
            return this;
        }

        public RequestInfoBuilder setRequestStartTime(final long requestStartTime) {
            this.requestStartTime = requestStartTime;
            return this;
        }

        public RequestInfo build() {
            return new RequestInfo(failedMessages, batchSize, requestStartTime);
        }
    }
}
