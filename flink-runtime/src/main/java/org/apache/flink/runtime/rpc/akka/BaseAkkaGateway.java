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

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.runtime.rpc.MainThreadExecutor;
import org.apache.flink.runtime.rpc.akka.messages.CallableMessage;
import org.apache.flink.runtime.rpc.akka.messages.RunnableMessage;
import scala.concurrent.Future;

import java.util.concurrent.Callable;

public abstract class BaseAkkaGateway implements MainThreadExecutor, AkkaGateway {
	@Override
	public void runAsync(Runnable runnable) {
		getActorRef().tell(new RunnableMessage(runnable), ActorRef.noSender());
	}

	@Override
	public <V> Future<V> callAsync(Callable<V> callable, Timeout timeout) {
		return (Future<V>) Patterns.ask(getActorRef(), new CallableMessage(callable), timeout);
	}
}
