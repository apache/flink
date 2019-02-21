package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.streaming.connectors.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;

/**
 * A PubSubSubscriberFactory that can be used to connect to a PubSub emulator.
 */
public class PubSubSubscriberFactoryForEmulator implements PubSubSubscriberFactory {
	private final String hostAndPort;

	public PubSubSubscriberFactoryForEmulator(String hostAndPort) {
		this.hostAndPort = hostAndPort;
	}

	@Override
	public SubscriberStub getSubscriber(Credentials credentials) throws IOException {
		ManagedChannel managedChannel = ManagedChannelBuilder
			.forTarget(hostAndPort)
			.usePlaintext() // This is 'Ok' because this is ONLY used for testing.
			.build();

		SubscriberStubSettings settings = SubscriberStubSettings.newBuilder()
																.setCredentialsProvider(NoCredentialsProvider.create())
																.setTransportChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(managedChannel)))
																.build();
		return GrpcSubscriberStub.create(settings);
	}
}
