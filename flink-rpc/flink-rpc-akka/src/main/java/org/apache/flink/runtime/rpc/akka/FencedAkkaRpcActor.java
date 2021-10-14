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

import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaUnknownMessageException;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.rpc.messages.FencedMessage;
import org.apache.flink.runtime.rpc.messages.LocalFencedMessage;
import org.apache.flink.runtime.rpc.messages.UnfencedMessage;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Fenced extension of the {@link AkkaRpcActor}. This actor will be started for {@link
 * FencedRpcEndpoint} and is responsible for filtering out invalid messages with respect to the
 * current fencing token.
 *
 * @param <F> type of the fencing token
 * @param <T> type of the RpcEndpoint
 */
public class FencedAkkaRpcActor<F extends Serializable, T extends FencedRpcEndpoint<F> & RpcGateway>
        extends AkkaRpcActor<T> {

    public FencedAkkaRpcActor(
            T rpcEndpoint,
            CompletableFuture<Boolean> terminationFuture,
            int version,
            final long maximumFramesize,
            ClassLoader flinkClassLoader) {
        super(rpcEndpoint, terminationFuture, version, maximumFramesize, flinkClassLoader);
    }

    @Override
    protected void handleRpcMessage(Object message) {
        if (message instanceof FencedMessage) {

            final F expectedFencingToken = rpcEndpoint.getFencingToken();

            if (expectedFencingToken == null) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Fencing token not set: Ignoring message {} because the fencing token is null.",
                            message);
                }

                sendErrorIfSender(
                        new FencingTokenException(
                                String.format(
                                        "Fencing token not set: Ignoring message %s sent to %s because the fencing token is null.",
                                        message, rpcEndpoint.getAddress())));
            } else {
                @SuppressWarnings("unchecked")
                FencedMessage<F, ?> fencedMessage = ((FencedMessage<F, ?>) message);

                F fencingToken = fencedMessage.getFencingToken();

                if (Objects.equals(expectedFencingToken, fencingToken)) {
                    super.handleRpcMessage(fencedMessage.getPayload());
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Fencing token mismatch: Ignoring message {} because the fencing token {} did "
                                        + "not match the expected fencing token {}.",
                                message,
                                fencingToken,
                                expectedFencingToken);
                    }

                    sendErrorIfSender(
                            new FencingTokenException(
                                    "Fencing token mismatch: Ignoring message "
                                            + message
                                            + " because the fencing token "
                                            + fencingToken
                                            + " did not match the expected fencing token "
                                            + expectedFencingToken
                                            + '.'));
                }
            }
        } else if (message instanceof UnfencedMessage) {
            super.handleRpcMessage(((UnfencedMessage<?>) message).getPayload());
        } else {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Unknown message type: Ignoring message {} because it is neither of type {} nor {}.",
                        message,
                        FencedMessage.class.getSimpleName(),
                        UnfencedMessage.class.getSimpleName());
            }

            sendErrorIfSender(
                    new AkkaUnknownMessageException(
                            "Unknown message type: Ignoring message "
                                    + message
                                    + " of type "
                                    + message.getClass().getSimpleName()
                                    + " because it is neither of type "
                                    + FencedMessage.class.getSimpleName()
                                    + " nor "
                                    + UnfencedMessage.class.getSimpleName()
                                    + '.'));
        }
    }

    @Override
    protected Object envelopeSelfMessage(Object message) {
        final F fencingToken = rpcEndpoint.getFencingToken();

        return new LocalFencedMessage<>(fencingToken, message);
    }
}
