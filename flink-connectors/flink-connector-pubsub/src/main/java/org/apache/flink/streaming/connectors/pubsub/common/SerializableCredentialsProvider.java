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

package org.apache.flink.streaming.connectors.pubsub.common;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;

import java.io.IOException;
import java.io.Serializable;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;

/**
 * Wrapper class for CredentialsProvider to make it Serializable. This can be used to pass on Credentials to SourceFunctions
 */
public class SerializableCredentialsProvider implements CredentialsProvider, Serializable {
	private final Credentials credentials;

	/**
	 * @param credentials The google {@link Credentials} needed to connect to PubSub
	 */
	public SerializableCredentialsProvider(Credentials credentials) {
		this.credentials = credentials;
	}

	/**
	 * Creates a SerializableCredentialsProvider for a PubSubSubscription based on environment variables.
	 * {@link com.google.cloud.pubsub.v1.SubscriptionAdminSettings}
	 *
	 * @return serializableCredentialsProvider
	 * @throws IOException thrown by {@link Credentials}
	 */
	public static SerializableCredentialsProvider credentialsProviderFromEnvironmentVariables() throws IOException {
		Credentials credentials = defaultCredentialsProviderBuilder().build().getCredentials();
		return new SerializableCredentialsProvider(credentials);
	}

	/**
	 * Creates a SerializableCredentialsProvider for a PubSubSubscription without any credentials.
	 * {@link com.google.cloud.pubsub.v1.SubscriptionAdminSettings}
	 * This is ONLY useful when running tests locally against Mockito or the Google PubSub emulator
	 * @see <a href="https://cloud.google.com/pubsub/docs/emulator" target="_top">https://cloud.google.com/pubsub/docs/emulator</a>
	 * @return serializableCredentialsProvider
	 * @see <a href="https://cloud.google.com/pubsub/docs/emulator" target="_top">https://cloud.google.com/pubsub/docs/emulator</a>
	 */
	public static SerializableCredentialsProvider withoutCredentials() {
		return new SerializableCredentialsProvider(NoCredentials.getInstance());
	}

	@Override
	public Credentials getCredentials() {
		return credentials;
	}
}
