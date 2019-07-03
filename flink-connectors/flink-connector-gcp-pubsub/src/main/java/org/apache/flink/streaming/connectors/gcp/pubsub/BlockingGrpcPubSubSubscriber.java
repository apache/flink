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

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;

import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implementation for {@link PubSubSubscriber}.
 * This Grpc PubSubSubscriber allows for flexible timeouts and retries.
 */
public class BlockingGrpcPubSubSubscriber implements PubSubSubscriber {
	private final String projectSubscriptionName;
	private final ManagedChannel channel;
	private final SubscriberGrpc.SubscriberBlockingStub stub;
	private final int retries;
	private final Duration timeout;
	private final PullRequest pullRequest;

	public BlockingGrpcPubSubSubscriber(String projectSubscriptionName,
										ManagedChannel channel,
										SubscriberGrpc.SubscriberBlockingStub stub,
										PullRequest pullRequest,
										int retries,
										Duration timeout) {
		this.projectSubscriptionName = projectSubscriptionName;
		this.channel = channel;
		this.stub = stub;
		this.retries = retries;
		this.timeout = timeout;
		this.pullRequest = pullRequest;
	}

	@Override
	public List<ReceivedMessage> pull() {
		return pull(retries);
	}

	private List<ReceivedMessage> pull(int retriesRemaining) {
		try {
			return stub.withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS)
					.pull(pullRequest)
					.getReceivedMessagesList();
		} catch (StatusRuntimeException e) {
			if (retriesRemaining > 0) {
				return pull(retriesRemaining - 1);
			}

			throw e;
		}
	}

	@Override
	public void acknowledge(List<String> acknowledgementIds) {
		if (acknowledgementIds.isEmpty()) {
			return;
		}

		//grpc servers won't accept acknowledge requests that are too large so we split the ackIds
		Tuple2<List<String>, List<String>> splittedAckIds = splitAckIds(acknowledgementIds);
		while (!splittedAckIds.f0.isEmpty()) {
			AcknowledgeRequest acknowledgeRequest =
					AcknowledgeRequest.newBuilder()
									.setSubscription(projectSubscriptionName)
									.addAllAckIds(splittedAckIds.f0)
									.build();

			stub.withDeadlineAfter(60, SECONDS).acknowledge(acknowledgeRequest);

			splittedAckIds = splitAckIds(splittedAckIds.f1);
		}
	}

	/* maxPayload is the maximum number of bytes to devote to actual ids in
	 * acknowledgement or modifyAckDeadline requests. A serialized
	 * AcknowledgeRequest grpc call has a small constant overhead, plus the size of the
	 * subscription name, plus 3 bytes per ID (a tag byte and two size bytes). A
	 * ModifyAckDeadlineRequest has an additional few bytes for the deadline. We
	 * don't know the subscription name here, so we just assume the size exclusive
	 * of ids is 100 bytes.

	 * With gRPC there is no way for the client to know the server's max message size (it is
	 * configurable on the server). We know from experience that it is 512K.
	 * @return First list contains no more than 512k bytes, second list contains remaining ids
	 */
	private Tuple2<List<String>, List<String>> splitAckIds(List<String> ackIds) {
		final int maxPayload = 500 * 1024; //little below 512k bytes to be on the safe side
		final int fixedOverheadPerCall = 100;
		final int overheadPerId = 3;

		int totalBytes = fixedOverheadPerCall;

		for (int i = 0; i < ackIds.size(); i++) {
			totalBytes += ackIds.get(i).length() + overheadPerId;
			if (totalBytes > maxPayload) {
				return Tuple2.of(ackIds.subList(0, i), ackIds.subList(i, ackIds.size()));
			}
		}

		return Tuple2.of(ackIds, emptyList());
	}

	@Override
	public void close() throws Exception {
		channel.shutdownNow();
		channel.awaitTermination(20, SECONDS);
	}
}
