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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.runtime.rpc.messages.FencedMessage;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.runtime.stabilitytest.StabilityTestException;
import org.apache.flink.runtime.stabilitytest.ZookeeperClientServices;
import org.apache.flink.runtime.stabilitytest.ZookeeperClientServices.NodeRetrievalListener;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.pattern.AskTimeoutException;
import akka.pattern.PromiseActorRef;
import akka.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import scala.concurrent.duration.Duration;
import scala.util.Failure;

import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_DEFAULT;
import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_FAULT_PROBABILITY;
import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_FAULT_TRIGGER;

/**
 * Stability test case class for rpc timeout, it simulate rpc's connect-timeout and ask-timeout
 * via the given probability.
 *
 * <p>A stability test case class what be called ST-class must implements the static method
 * void initialize(Object arg0, Object arg1).
 *
 * <p>A stability test case what be called ST-case is one static method or the assemble of more
 * static methods in a ST-class. The first parameter of every ST-case method must be Boolean[],
 * it is a array contained one element and tell the caller whether step over all behind codes of
 * the injectable target method after invoke the ST-case method.
 */
public final class AkkaRpcTimeoutSTCase {
	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcTimeoutSTCase.class);

	/** The Random() instance for connect timeout. */
	private static final Random connRandom = new Random();

	/** The Random() instance for ask timeout. */
	private static final Random askRandom = new Random();

	/** The upper bound of random. */
	private static final int RANDOM_BOUND = Integer.MAX_VALUE;

	/** Probability of connect-timeout. */
	private static volatile double connTimeoutProb = 0.0;

	/** Probability of ask-timeout. */
	private static volatile double askTimeoutProb = 0.0;

	/** Zookeeper node for rpc's connect-timeout control. */
	public static final String CONNECT_TIMEOUT_ZKNODE_PATH = "/rpc_timeout/connect";

	/** Zookeeper node for rpc's ask-timeout control. */
	public static final String ASK_TIMEOUT_ZKNODE_PATH = "/rpc_timeout/ask";

	private static NodeRetrievalListener connTimeoutProbListener = new NodeRetrievalListener() {
		@Override
		public void notify(String data, Throwable throwable) {
			double oldTimeoutProb = connTimeoutProb;

			if (data == null || throwable != null) {
				connTimeoutProb = 0.0;
			} else {
				try {
					double prob = Double.parseDouble(data);
					connTimeoutProb = Math.max(0.0, Math.min(1.0, prob));
				} catch (NumberFormatException nfe) {
					LOG.error("{} update rpc-connect timeout probability failed, ignore this value: {}."
						, LOG_PREFIX_FAULT_PROBABILITY, data);

					return;
				}
			}

			LOG.info("{} update rpc-connect timeout probability from {} to {}."
				, LOG_PREFIX_FAULT_PROBABILITY, oldTimeoutProb, connTimeoutProb);
		}
	};

	private static NodeRetrievalListener askTimeoutProbListener = new NodeRetrievalListener() {
		@Override
		public void notify(String data, Throwable throwable) {
			double oldTimeoutProb = askTimeoutProb;

			if (data == null || throwable != null) {
				askTimeoutProb = 0.0;
			} else {
				try {
					double prob = Double.parseDouble(data);
					askTimeoutProb = Math.max(0.0, Math.min(1.0, prob));
				} catch (NumberFormatException nfe) {
					LOG.error("{} update rpc-ask timeout probability failed, ignore this value: {}."
						, LOG_PREFIX_FAULT_PROBABILITY, data);

					return;
				}
			}

			LOG.info("{} update rpc-ask timeout probability from {} to {}."
				, LOG_PREFIX_FAULT_PROBABILITY, oldTimeoutProb, askTimeoutProb);
		}
	};

	// For debug probability value temporary
	private static final AtomicLong rpcCounter = new AtomicLong(0);

	/**
	 * Connection to the ZooKeeper quorum, register connect-timout and ask-timout listeners.
	 * @param arg0 is a Properties object to initialize Zookeeper client settings
	 * @param arg1 is the prefix of property key which should be replaced
	 * @throws Exception
	 */
	public static void initialize(Object arg0, Object arg1) throws StabilityTestException {
		Preconditions.checkArgument(arg0 != null && arg0 instanceof Properties);
		Preconditions.checkArgument(arg1 == null || arg1 instanceof String);
		Properties properties = (Properties) arg0;
		String replacedPrefix = (String) arg1;

		try {
			if (!ZookeeperClientServices.isStarted()) {
				ZookeeperClientServices.start(new ZookeeperClientServices.Configuration(properties, replacedPrefix), true);
			}

			if (!ZookeeperClientServices.isListenerRegistered(connTimeoutProbListener)) {
				ZookeeperClientServices.registerListener(connTimeoutProbListener, CONNECT_TIMEOUT_ZKNODE_PATH, true);
			}

			if (!ZookeeperClientServices.isListenerRegistered(askTimeoutProbListener)) {
				ZookeeperClientServices.registerListener(askTimeoutProbListener, ASK_TIMEOUT_ZKNODE_PATH, true);
			}
		} catch (Throwable t) {
			throw new StabilityTestException(t);
		}
	}

	/**
	 * Simulate rpc connection timeout by the given probability.
	 * @param isEarlyReturn tell the ST-Case helper whether return directly after invoke ST-Case method
	 * @param arg0 is a ActorSelection object to connect the rpc-endpoint
	 * @param message the serialized message body to send
	 * @param arg2 is the sender which's type is ActorRef
	 */
	public static void connectTimeout(Boolean[] isEarlyReturn, Object arg0, Object message, Object arg2) throws StabilityTestException {
		Preconditions.checkArgument(arg0 != null && arg0 instanceof ActorSelection);
		Preconditions.checkArgument(arg2 == null || arg2 instanceof ActorRef);
		ActorSelection actorSel = (ActorSelection) arg0;
		ActorRef sender = (ActorRef) arg2;

		try {
			isEarlyReturn[0] = Boolean.TRUE; // Always intercept the target method

			if (connTimeoutProb <= 0) {
				actorSel.tell(message, sender);
				return;
			}

			TimeoutType timeoutType = null;
			int rnd = connRandom.nextInt(RANDOM_BOUND);
			if (rnd <= RANDOM_BOUND * connTimeoutProb) {
				if (rnd % 2 == 0) { // request timeout
					timeoutType = TimeoutType.REQUEST;
				} else { // response timeout
					timeoutType = TimeoutType.RESPONSE;
				}
			}
			if (timeoutType == null) {
				actorSel.tell(message, sender);
				return;
			}

			// Hit the probability and trigger timeout on purpose
			if (sender instanceof PromiseActorRef) {
				PromiseActorRef a = (PromiseActorRef) sender;
				a.result().complete(new Failure(new AskTimeoutException(
						String.format("%s rpc-connect[%s] timeout, message: %s."
								, LOG_PREFIX_FAULT_TRIGGER, timeoutType.name(), tryRpcInvocationToMethod(message))
				)));

				if (timeoutType == TimeoutType.RESPONSE) {
					/**
					 * Use pseudo fire-and-forget semantics
					 * The value of timeout using {@link AkkaOptions.ASK_TIMEOUT.defaultValue}
					 */
					Timeout timeout = new Timeout(Duration.apply(AkkaOptions.ASK_TIMEOUT.defaultValue()).toMillis(), TimeUnit.MILLISECONDS);
					actorSel.tell(message, PromiseActorRef.apply(a.provider(), timeout, actorSel.toString()));
				}
			} else if (sender == null) {
				// Do'nt send message for Actor.noSender and logging timeout fault
				LOG.error("{} rpc-connect[{}] timeout, message: {}."
					, LOG_PREFIX_FAULT_TRIGGER, TimeoutType.REQUEST, tryRpcInvocationToMethod(message));
			} else {
				throw new UnsupportedOperationException(
						String.format("%s not support the sender class: %s, message: %s."
								, LOG_PREFIX_DEFAULT, sender.getClass().getCanonicalName(), tryRpcInvocationToMethod(message)));
			}
		} catch (Throwable t) {
			throw new StabilityTestException(t);
		}
	}

	/**
	 * Simulate rpc ask timeout by the given probability.
	 * @param isEarlyReturn tell the ST-Case helper whether return directly after invoke ST-Case method
	 * @param arg0 is a ActorRef object to ask the rpc-endpoint
	 * @param message the serialized message body to send
	 * @param arg2 is the sender which's type is ActorRef
	 */
	public static void askTimeout(Boolean[] isEarlyReturn, Object arg0, Object message, Object arg2) throws StabilityTestException {
		Preconditions.checkArgument(arg0 != null && arg0 instanceof ActorRef);
		Preconditions.checkArgument(arg2 == null || arg2 instanceof ActorRef);
		ActorRef actorRef = (ActorRef) arg0;
		ActorRef sender = (ActorRef) arg2;

		try {
			isEarlyReturn[0] = Boolean.TRUE; // Always intercept the target method

			// For debug probability value temporary
			Long rpcCount = rpcCounter.incrementAndGet();
			if (rpcCount % 1000 == 0) {
				LOG.info("{} debug probability temporary, rpc count: {}", LOG_PREFIX_DEFAULT, rpcCount);
			}

			if (askTimeoutProb <= 0) {
				actorRef.tell(message, sender);
				return;
			}

			TimeoutType timeoutType = null;
			int randomNum = askRandom.nextInt(RANDOM_BOUND);
			if (randomNum <= RANDOM_BOUND * askTimeoutProb) {
				if (randomNum % 2 == 0) { // request timeout
					timeoutType = TimeoutType.REQUEST;
				} else { // response timeout
					timeoutType = TimeoutType.RESPONSE;
				}
			}
			if (timeoutType == null) {
				actorRef.tell(message, sender);
				return;
			}

			// Hit the probability and trigger timeout on purpose
			if (message != null &&
				(message instanceof RpcInvocation ||
					(message instanceof FencedMessage && ((FencedMessage) message).getPayload() instanceof RpcInvocation))) {
				if (sender instanceof PromiseActorRef) {
					PromiseActorRef a = (PromiseActorRef) sender;
					a.result().complete(new Failure(new AskTimeoutException(
							String.format("%s rpc-ask[%s] timeout, message: %s."
									, LOG_PREFIX_FAULT_TRIGGER, timeoutType.name(), tryRpcInvocationToMethod(message))
					)));

					if (timeoutType == TimeoutType.RESPONSE) {
						// Use fire-and-forget semantics
						actorRef.tell(message, null);
					}

					return;
				} else if (sender == null) {
					// Do'nt send message for Actor.noSender and logging timeout fault
					LOG.error("{} rpc-ask[{}] timeout, message: {}."
						, LOG_PREFIX_FAULT_TRIGGER, TimeoutType.REQUEST, tryRpcInvocationToMethod(message));

					return;
				}
			}

			// Logging a prompt message
			LOG.info("{} rpc-ask[{}] not go out of time for sender: {}, message: {}."
						, LOG_PREFIX_DEFAULT, timeoutType.name(), (sender == null) ? null : sender.getClass().getCanonicalName(), tryRpcInvocationToMethod(message));

			// All the other cases, not go out of time
			actorRef.tell(message, sender);
		} catch (Throwable t) {
			throw new StabilityTestException(t);
		}
	}

	/**
	 * Try to extract rpc method from the message.
	 * @param message is the rpc message body
	 * @return formatted rpc method
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static String tryRpcInvocationToMethod(Object message) throws IOException, ClassNotFoundException {
		if (message == null) {
			return null;
		}

		StringBuffer buffer = new StringBuffer();

		Object payload = null;
		if (message instanceof FencedMessage) {
			FencedMessage fencedMessage = (FencedMessage) message;
			buffer.append(fencedMessage.getClass().getSimpleName())
				.append("(")
				.append("fencingToken: ")
				.append(fencedMessage.getFencingToken())
				.append(", ");

			payload = fencedMessage.getPayload();
		} else {
			payload = message;
		}

		if (payload != null && payload instanceof RpcInvocation) {
			RpcInvocation rpcInvocation = (RpcInvocation) payload;
			buffer.append(rpcInvocation.getClass().getSimpleName())
				.append("(")
				.append("called method: ")
				.append(rpcInvocation.getMethodName())
				.append("(");

			Class<?>[] parameterTypes = rpcInvocation.getParameterTypes();
			if (parameterTypes != null) {
				for (int i = 0; i < parameterTypes.length; i++) {
					if (i > 0) {
						buffer.append(", ");
					}
					buffer.append(parameterTypes[i].getSimpleName());
				}
			}

			buffer.append(")")
				.append(")");
		} else {
			buffer.append(String.format("[unknown class: %s]", payload == null ? null : payload.getClass().getCanonicalName()));
		}

		if (message instanceof FencedMessage) {
			buffer.append(")");
		}

		return buffer.toString();
	}

	enum TimeoutType {
		REQUEST,
		RESPONSE
	}
}
