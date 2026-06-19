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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.MainThreadExecutable;
import org.apache.flink.runtime.rpc.messages.FencedMessage;
import org.apache.flink.runtime.rpc.messages.LocalFencedMessage;
import org.apache.flink.runtime.rpc.messages.RemoteFencedMessage;
import org.apache.flink.util.Preconditions;

import org.apache.pekko.actor.ActorRef;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Fenced extension of the {@link PekkoInvocationHandler}. This invocation handler will be used in
 * combination with the {@link FencedRpcEndpoint}. The fencing is done by wrapping all messages in a
 * {@link FencedMessage}.
 *
 * @param <F> type of the fencing token
 */
public class FencedPekkoInvocationHandler<F extends Serializable> extends PekkoInvocationHandler
        implements MainThreadExecutable, FencedRpcGateway<F> {

    private final Supplier<F> fencingTokenSupplier;

    public FencedPekkoInvocationHandler(
            String address,
            String hostname,
            ActorRef rpcEndpoint,
            Duration timeout,
            long maximumFramesize,
            boolean forceRpcInvocationSerialization,
            @Nullable CompletableFuture<Void> terminationFuture,
            Supplier<F> fencingTokenSupplier,
            boolean captureAskCallStacks,
            ClassLoader flinkClassLoader) {
        super(
                address,
                hostname,
                rpcEndpoint,
                timeout,
                maximumFramesize,
                forceRpcInvocationSerialization,
                terminationFuture,
                captureAskCallStacks,
                flinkClassLoader);

        this.fencingTokenSupplier = Preconditions.checkNotNull(fencingTokenSupplier);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Class<?> declaringClass = method.getDeclaringClass();

        if (declaringClass.equals(MainThreadExecutable.class)
                || declaringClass.equals(FencedRpcGateway.class)) {
            return method.invoke(this, args);
        } else {
            return super.invoke(proxy, method, args);
        }
    }

    @Override
    public void tell(Object message) {
        super.tell(fenceMessage(message));
    }

    @Override
    public CompletableFuture<?> ask(Object message, Duration timeout) {
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
                FencedMessage<F, P> result =
                        (FencedMessage<F, P>)
                                new RemoteFencedMessage<>(
                                        fencingTokenSupplier.get(), (Serializable) message);

                return result;
            } else {
                throw new RuntimeException(
                        "Trying to send a non-serializable message "
                                + message
                                + " to a remote "
                                + "RpcEndpoint. Please make sure that the message implements java.io.Serializable.");
            }
        }
    }
}
