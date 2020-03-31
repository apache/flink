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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.FencedMainThreadExecutable;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.messages.CallAsync;
import org.apache.flink.runtime.rpc.messages.FencedMessage;
import org.apache.flink.runtime.rpc.messages.LocalFencedMessage;
import org.apache.flink.runtime.rpc.messages.RemoteFencedMessage;
import org.apache.flink.runtime.rpc.messages.RunAsync;
import org.apache.flink.runtime.rpc.messages.UnfencedMessage;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.pattern.Patterns;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Fenced extension of the {@link AkkaInvocationHandler}. This invocation handler will be used in combination
 * with the {@link FencedRpcEndpoint}. The fencing is done by wrapping all messages in a {@link FencedMessage}.
 *
 * @param <F> type of the fencing token
 */
public class FencedAkkaInvocationHandler<F extends Serializable> extends AkkaInvocationHandler implements FencedMainThreadExecutable, FencedRpcGateway<F> {

	private final Supplier<F> fencingTokenSupplier;

	public FencedAkkaInvocationHandler(
			String address,
			String hostname,
			ActorRef rpcEndpoint,
			Time timeout,
			long maximumFramesize,
			@Nullable CompletableFuture<Void> terminationFuture,
			Supplier<F> fencingTokenSupplier) {
		super(address, hostname, rpcEndpoint, timeout, maximumFramesize, terminationFuture);

		this.fencingTokenSupplier = Preconditions.checkNotNull(fencingTokenSupplier);
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?> declaringClass = method.getDeclaringClass();

		if (declaringClass.equals(FencedMainThreadExecutable.class) ||
			declaringClass.equals(FencedRpcGateway.class)) {
			return method.invoke(this, args);
		} else {
			return super.invoke(proxy, method, args);
		}
	}

	@Override
	public void runAsyncWithoutFencing(Runnable runnable) {
		checkNotNull(runnable, "runnable");

		if (isLocal) {
			getActorRef().tell(
				new UnfencedMessage<>(new RunAsync(runnable, 0L)), ActorRef.noSender());
		} else {
			throw new RuntimeException("Trying to send a Runnable to a remote actor at " +
				getActorRef().path() + ". This is not supported.");
		}
	}

	@Override
	public <V> CompletableFuture<V> callAsyncWithoutFencing(Callable<V> callable, Time timeout) {
		checkNotNull(callable, "callable");
		checkNotNull(timeout, "timeout");

		if (isLocal) {
			@SuppressWarnings("unchecked")
			CompletableFuture<V> resultFuture = (CompletableFuture<V>) FutureUtils.toJava(
				Patterns.ask(
					getActorRef(),
					new UnfencedMessage<>(new CallAsync(callable)),
					timeout.toMilliseconds()));

			return resultFuture;
		} else {
			throw new RuntimeException("Trying to send a Runnable to a remote actor at " +
				getActorRef().path() + ". This is not supported.");
		}
	}

	@Override
	public void tell(Object message) {
		super.tell(fenceMessage(message));
	}

	@Override
	public CompletableFuture<?> ask(Object message, Time timeout) {
		return super.ask(fenceMessage(message), timeout);
	}

	@Override
	public F getFencingToken() {
		return fencingTokenSupplier.get();
	}

	private <P> FencedMessage<F, P> fenceMessage(P message) {
		if (isLocal) {
			return new LocalFencedMessage<>(fencingTokenSupplier.get(), message);
		} else {
			if (message instanceof Serializable) {
				@SuppressWarnings("unchecked")
				FencedMessage<F, P> result = (FencedMessage<F, P>) new RemoteFencedMessage<>(fencingTokenSupplier.get(), (Serializable) message);

				return result;
			} else {
				throw new RuntimeException("Trying to send a non-serializable message " + message + " to a remote " +
					"RpcEndpoint. Please make sure that the message implements java.io.Serializable.");
			}
		}
	}
}
