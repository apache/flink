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

import akka.actor.ActorRef;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * Interface to abstract the communication with an Instance.
 *
 * It allows to avoid direct interaction with an ActorRef.
 */
public interface InstanceGateway {

	/**
	 * Sends a message asynchronously and returns its response. The response to the message is
	 * returned as a future.
	 *
	 * @param message Message to be sent
	 * @param timeout Timeout until the Future is completed with an AskTimeoutException
	 * @return Future which contains the response to the sent message
	 */
	Future<Object> ask(Object message, FiniteDuration timeout);

	/**
	 * Sends a message asynchronously without a result.
	 *
	 * @param message Message to be sent
	 */
	void tell(Object message);

	/**
	 * Forwards a message. For the receiver of this message it looks as if sender has sent the
	 * message.
	 *
	 * @param message Message to be sent
	 * @param sender Sender of the forwarded message
	 */
	void forward(Object message, ActorRef sender);

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
	Future<Object> retry(
			Object message,
			int numberRetries,
			FiniteDuration timeout,
			ExecutionContext executionContext);

	/**
	 * Returns the path of the remote instance.
	 *
	 * @return Path of the remote instance.
	 */
	String path();
}
