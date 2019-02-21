/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.streaming.connectors.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;

import java.io.IOException;

class DefaultPubSubSubscriberFactory implements PubSubSubscriberFactory {
	@Override
	public SubscriberStub getSubscriber(Credentials credentials) throws IOException {
		SubscriberStubSettings settings = SubscriberStubSettings.newBuilder()
																.setCredentialsProvider(FixedCredentialsProvider.create(credentials))
																.build();

		return GrpcSubscriberStub.create(settings);
	}
}
