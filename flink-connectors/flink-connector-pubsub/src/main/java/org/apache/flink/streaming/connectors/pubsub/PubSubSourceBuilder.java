package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.pubsub.v1.ProjectSubscriptionName;

import java.io.IOException;

/**
 * Factory class to create a PubSubSource with custom options.
 */
public class PubSubSourceBuilder<OUT> {
	private SerializableCredentialsProvider serializableCredentialsProvider;
	private DeserializationSchema<OUT>      deserializationSchema;
	private String                          projectName;
	private String                          subscriptionName;
	private String                          hostAndPort;
	private Mode                            mode;

	public static <OUT> PubSubSourceBuilder<OUT> builder() {
		return new PubSubSourceBuilder<>();
	}

	/**
	 * Possible modes which represent the guarantees given by the PubSubSource.
	 */
	public enum Mode {
		NONE, ATLEAST_ONCE, EXACTLY_ONCE
	}

	/**
	 * Set the mode, possible values NONE, ATLEAST_ONCE, EXACTLY_ONCE.
	 * EXACTLY_ONCE will run with a parallelism of 1.
	 * @param mode the mode to use
	 * @return The current Builder instance
	 */
	public PubSubSourceBuilder<OUT> withMode(Mode mode) {
		this.mode = mode;
		return this;
	}

	/**
	 * Set the credentials.
	 * If this is not used then the credentials are picked up from the environment variables.
	 * @param credentials the Credentials needed to connect.
	 * @return The current Builder instance
	 */
	public PubSubSourceBuilder<OUT> withCredentials(Credentials credentials) {
		this.serializableCredentialsProvider = new SerializableCredentialsProvider(credentials);
		return this;
	}

	/**
	 * Set the CredentialsProvider.
	 * If this is not used then the credentials are picked up from the environment variables.
	 * @param credentialsProvider the custom SerializableCredentialsProvider instance.
	 * @return The current Builder instance
	 */
	public PubSubSourceBuilder<OUT> withCredentialsProvider(CredentialsProvider credentialsProvider) throws IOException {
		return withCredentials(credentialsProvider.getCredentials());
	}

	/**
	 * @param deserializationSchema Instance of a DeserializationSchema that converts the OUT into a byte[]
	 * @return The current Builder instance
	 */
	public PubSubSourceBuilder<OUT> withDeserializationSchema(DeserializationSchema<OUT> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
		return this;
	}

	/**
	 * @param projectName The name of the project in GoogleCloudPlatform
	 * @param subscriptionName The name of the subscription in PubSub
	 * @return The current Builder instance
	 */
	public PubSubSourceBuilder<OUT> withProjectSubscriptionName(String projectName, String subscriptionName) {
		this.projectName = projectName;
		this.subscriptionName = subscriptionName;
		return this;
	}

	/**
	 * Set the custom hostname/port combination of PubSub.
	 * The ONLY reason to use this is during tests with the emulator provided by Google.
	 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
	 * @return The current instance
	 */
	public PubSubSourceBuilder<OUT> withHostAndPort(String hostAndPort) {
		this.hostAndPort = hostAndPort;
		return this;
	}

	/**
	 * Actually build the desired instance of the PubSubSourceBuilder.
	 * @return a brand new PubSubSource
	 * @throws IOException incase of a problem getting the credentials
	 * @throws IllegalArgumentException incase required fields were not specified.
	 */
	public SourceFunction<OUT> build() throws IOException {
		if (serializableCredentialsProvider == null) {
			serializableCredentialsProvider = SerializableCredentialsProvider.credentialsProviderFromEnvironmentVariables();
		}
		if (deserializationSchema == null) {
			throw new IllegalArgumentException("The deserializationSchema has not been specified.");
		}
		if (projectName == null || subscriptionName == null) {
			throw new IllegalArgumentException("The ProjectName And SubscriptionName has not been specified.");
		}
		if (mode == null) {
			throw new IllegalArgumentException("The mode has not been specified");
		}

		SubscriberWrapper subscriberWrapper =
			new SubscriberWrapper(serializableCredentialsProvider, ProjectSubscriptionName.of(projectName, subscriptionName));

		if (hostAndPort != null) {
			subscriberWrapper.withHostAndPort(hostAndPort);
		}

		switch(mode) {
			default:
			case NONE:
			case ATLEAST_ONCE:
				return new ParallelPubSubSource<>(subscriberWrapper, deserializationSchema, mode);
			case EXACTLY_ONCE:
				return new PubSubSource<>(subscriberWrapper, deserializationSchema, mode);
		}
	}
}
