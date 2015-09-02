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

import java.util.UUID;

/**
 * Dummy {@link ActorGateway} implementation used for testing.
 */
public class DummyActorGateway implements ActorGateway {
	public static final DummyActorGateway INSTANCE = new DummyActorGateway();

	@Override
	public Future<Object> ask(Object message, FiniteDuration timeout) {
		return null;
	}

	@Override
	public void tell(Object message) {}

	@Override
	public void tell(Object message, ActorGateway sender) {}

	@Override
	public void forward(Object message, ActorGateway sender) {}

	@Override
	public Future<Object> retry(Object message, int numberRetries, FiniteDuration timeout, ExecutionContext executionContext) {
		return null;
	}

	@Override
	public String path() {
		return "DummyInstanceGateway";
	}

	@Override
	public ActorRef actor() {
		return ActorRef.noSender();
	}

	@Override
	public UUID leaderSessionID() {
		return null;
	}
}
