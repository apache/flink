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

package org.apache.flink.runtime.instance;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.Identify;
import akka.dispatch.OnComplete;
import akka.dispatch.OnSuccess;
import akka.pattern.AskTimeoutException;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.messages.LeaderSessionMessageDecorator;
import org.apache.flink.runtime.messages.MessageDecorator;
import org.apache.flink.util.Preconditions;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Concrete {@link ActorGateway} implementation which uses Akka to communicate with remote actors.
 */
public class AkkaActorGateway implements ActorGateway {

	// ActorRef of the remote instance
	private final ActorRef actor;

	// Associated leader session ID, which is used for RequiresLeaderSessionID messages
	private final UUID leaderSessionID;

	// Execution context for the future callbacks
	private final ExecutionContext executionContext;

	// Decorator for messages
	private final MessageDecorator decorator;

	public AkkaActorGateway(ActorRef actor, UUID leaderSessionID, ExecutionContext executionContext) {
		this.actor = Preconditions.checkNotNull(actor);
		this.leaderSessionID = leaderSessionID;
		this.executionContext = Preconditions.checkNotNull(executionContext);
		// we want to wrap RequiresLeaderSessionID messages in a LeaderSessionMessage
		this.decorator = new LeaderSessionMessageDecorator(leaderSessionID);
	}

	/**
	 * Sends a message asynchronously and returns its response. The response to the message is
	 * returned as a future.
	 * <p>
	 * In order to detect network connection problems early, an identify message is sent to the
	 * target as well. If the identify returns with an empty response, then the returned future
	 * is failed early with a {@link TimeoutException}.
	 *
	 * @param message Message to be sent
	 * @param timeout Timeout until the Future is completed with an AskTimeoutException
	 * @return Future which contains the response to the sent message
	 */
	@Override
	public Future<Object> ask(Object message, FiniteDuration timeout) {
		Object newMessage = decorator.decorate(message);
		final Promise<Object> promise = new scala.concurrent.impl.Promise.DefaultPromise<>();

		Future<Object> identityFuture = Patterns.ask(actor, new Identify(true), new Timeout(timeout));

		identityFuture.onSuccess(new OnSuccess<Object>() {
			@Override
			public void onSuccess(Object result) {
				if (result instanceof ActorIdentity) {
					ActorIdentity actorIdentity = (ActorIdentity) result;

					if (actorIdentity.getRef() == null) {
						promise.failure(new AskTimeoutException("Akka identify message could not be resolved. " +
							"The remote host seems to be not reachable."));
					}
				} else {
					// We have not received an ActorIdentity response. Let's fail the ask operation,
					// because this indicates an inconsistent state
					promise.failure(new IllegalArgumentException("Expected to receive an ActorIdentity " +
						"response. Instead, received " + result + '.'));
				}
			}
		}, executionContext);

		Future<Object> askFuture = Patterns.ask(actor, newMessage, new Timeout(timeout));

		askFuture.onComplete(new OnComplete<Object>() {
			@Override
			public void onComplete(Throwable failure, Object success) throws Throwable {
				if (failure != null) {
					promise.failure(failure);
				} else {
					promise.success(success);
				}
			}
		}, executionContext);

		return promise.future();
	}

	/**
	 * Sends a message asynchronously without a result.
	 *
	 * @param message Message to be sent
	 */
	@Override
	public void tell(Object message) {
		Object newMessage = decorator.decorate(message);
		actor.tell(newMessage, ActorRef.noSender());
	}

	/**
	 * Sends a message asynchronously without a result with sender being the sender.
	 *
	 * @param message Message to be sent
	 * @param sender Sender of the message
	 */
	@Override
	public void tell(Object message, ActorGateway sender) {
		Object newMessage = decorator.decorate(message);
		actor.tell(newMessage, sender.actor());
	}

	/**
	 * Forwards a message. For the receiver of this message it looks as if sender has sent the
	 * message.
	 *
	 * @param message Message to be sent
	 * @param sender Sender of the forwarded message
	 */
	@Override
	public void forward(Object message, ActorGateway sender) {
		Object newMessage = decorator.decorate(message);
		actor.tell(newMessage, sender.actor());
	}

	/**
	 * Retries to send asynchronously a message up to numberRetries times. The response to this
	 * message is returned as a future. The message is re-sent if the number of retries is not yet
	 * exceeded and if an exception occurred while sending it.
	 *
	 * @param message Message to be sent
	 * @param numberRetries Number of times to retry sending the message
	 * @param timeout Timeout for each sending attempt
	 * @param executionContext ExecutionContext which is used to send the message multiple times
	 * @return Future of the response to the sent message
	 */
	@Override
	public Future<Object> retry(
			Object message,
			int numberRetries,
			FiniteDuration timeout,
			ExecutionContext executionContext) {

		Object newMessage = decorator.decorate(message);

		return AkkaUtils.retry(
			actor,
			newMessage,
			numberRetries,
			executionContext,
			timeout);
	}

	/**
	 * Returns the ActorPath of the remote instance.
	 *
	 * @return ActorPath of the remote instance.
	 */
	@Override
	public String path() {
		return actor.path().toString();
	}

	/**
	 * Returns {@link ActorRef} of the target actor
	 *
	 * @return ActorRef of the target actor
	 */
	@Override
	public ActorRef actor() {
		return actor;
	}

	@Override
	public UUID leaderSessionID() {
		return leaderSessionID;
	}

	@Override
	public String toString() {
		return String.format("AkkaActorGateway(%s, %s)", actor.path(), leaderSessionID);
	}
}
