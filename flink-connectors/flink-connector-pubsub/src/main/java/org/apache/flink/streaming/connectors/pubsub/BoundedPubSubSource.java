package org.apache.flink.streaming.connectors.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;

class BoundedPubSubSource<OUT> extends PubSubSource<OUT> {
	private Bound<OUT> bound;

	private BoundedPubSubSource() {
		super();
	}

	protected void setBound(Bound<OUT> bound) {
		this.bound = bound;
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) {
		bound.start(this);
		super.run(sourceContext);
	}

	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		super.receiveMessage(message, consumer);
		bound.receivedMessage();
	}

	@SuppressWarnings("unchecked")
	public static <OUT> BoundedPubSubSourceBuilder<OUT, ? extends PubSubSource, ? extends BoundedPubSubSourceBuilder> newBuilder() {
		return new BoundedPubSubSourceBuilder<>(new BoundedPubSubSource<OUT>());
	}

	@SuppressWarnings("unchecked")
	public static class BoundedPubSubSourceBuilder<OUT, PSS extends BoundedPubSubSource<OUT>, BUILDER extends BoundedPubSubSourceBuilder<OUT, PSS, BUILDER>> extends PubSubSourceBuilder<OUT, PSS, BUILDER> {
		private Long boundedByAmountOfMessages;
		private Long boundedByTimeSinceLastMessage;

		BoundedPubSubSourceBuilder(PSS sourceUnderConstruction) {
			super(sourceUnderConstruction);
		}

		public BUILDER boundedByAmountOfMessages(long maxAmountOfMessages) {
			boundedByAmountOfMessages = maxAmountOfMessages;
			return (BUILDER) this;
		}

		public BUILDER boundedByTimeSinceLastMessage(long timeSinceLastMessage) {
			boundedByTimeSinceLastMessage = timeSinceLastMessage;
			return (BUILDER) this;
		}

		private Bound <OUT> createBound() {
			if (boundedByAmountOfMessages != null && boundedByTimeSinceLastMessage != null) {
				return Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(boundedByAmountOfMessages, boundedByTimeSinceLastMessage);
			}

			if (boundedByAmountOfMessages != null) {
				return Bound.boundByAmountOfMessages(boundedByAmountOfMessages);
			}

			if (boundedByTimeSinceLastMessage != null) {
				return Bound.boundByTimeSinceLastMessage(boundedByTimeSinceLastMessage);
			}

			// This is functionally speaking no bound.
			return Bound.boundByAmountOfMessages(Long.MAX_VALUE);
		}

		@Override
		public PSS build() throws IOException {
			sourceUnderConstruction.setBound(createBound());
			return super.build();
		}
	}

}
