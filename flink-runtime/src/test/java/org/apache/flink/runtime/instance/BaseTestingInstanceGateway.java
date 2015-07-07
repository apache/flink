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

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.dispatch.Futures;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.Callable;

/**
 * Abstract base class for testing {@link InstanceGateway} instances. The implementing subclass
 * only has to provide an implementation for handleMessage which contains the logic to treat
 * different messages.
 */
abstract public class BaseTestingInstanceGateway implements InstanceGateway {
	/**
	 * {@link ExecutionContext} which is used to execute the futures.
	 */
	private final ExecutionContext executionContext;

	public BaseTestingInstanceGateway(ExecutionContext executionContext) {
		this.executionContext = executionContext;
	}

	@Override
	public Future<Object> ask(Object message, FiniteDuration timeout) {
		try {
			final Object result = handleMessage(message);

			return Futures.future(new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					return result;
				}
			}, executionContext);

		} catch (final Exception e) {
			// if an exception occurred in the handleMessage method then return it as part of the future
			return Futures.future(new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					throw e;
				}
			}, executionContext);
		}
	}

	/**
	 * Handles the supported messages by this InstanceGateway
	 *
	 * @param message Message to handle
	 * @return Result
	 * @throws Exception
	 */
	abstract public Object handleMessage(Object message) throws Exception;

	@Override
	public void tell(Object message) {}

	@Override
	public void forward(Object message, ActorRef sender) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<Object> retry(Object message, int numberRetries, FiniteDuration timeout, ExecutionContext executionContext) {
		return ask(message, timeout);
	}

	@Override
	public String path() {
		throw new UnsupportedOperationException();
	}
}
