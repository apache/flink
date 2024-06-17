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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.core.state.StateFutureImpl;

/**
 * A state future that holds the {@link RecordContext} and maintains the reference count of it. The
 * reason why we maintain the reference here is that the ContextStateFutureImpl can be created
 * multiple times since user may chain their code wildly, some of which are only for internal usage
 * (See {@link StateFutureImpl}). So maintaining reference counting by the lifecycle of state future
 * is relatively simple and less error-prone.
 *
 * <p>Reference counting added on {@link RecordContext} follows:
 * <li>1. +1 when this future created.
 * <li>2. -1 when future completed.
 * <li>3. +1 when callback registered.
 * <li>4. -1 when callback finished.
 * <li>Please refer to {@code ContextStateFutureImplTest} where the reference counting is carefully
 *     tested.
 */
public class ContextStateFutureImpl<T> extends StateFutureImpl<T> {

    private final RecordContext<?> recordContext;

    ContextStateFutureImpl(
            CallbackRunner callbackRunner,
            AsyncFrameworkExceptionHandler exceptionHandler,
            RecordContext<?> recordContext) {
        super(callbackRunner, exceptionHandler);
        this.recordContext = recordContext;
        // When state request submitted, ref count +1, as described in FLIP-425:
        // To cover the statements without a callback, in addition to the reference count marked
        // in Fig.5, each state request itself is also protected by a paired reference count.
        recordContext.retain();
    }

    @Override
    public <A> StateFutureImpl<A> makeNewStateFuture() {
        return new ContextStateFutureImpl<>(callbackRunner, exceptionHandler, recordContext);
    }

    @Override
    public void callbackRegistered() {
        // When a callback registered, as shown in Fig.5 of FLIP-425, at the point of 3 and 5, the
        // ref count +1.
        recordContext.retain();
    }

    @Override
    public void postComplete(boolean inCallbackRunner) {
        // When a state request completes, ref count -1, as described in FLIP-425:
        // To cover the statements without a callback, in addition to the reference count marked
        // in Fig.5, each state request itself is also protected by a paired reference count.
        if (inCallbackRunner) {
            recordContext.release(Runnable::run);
        } else {
            recordContext.release(
                    runnable -> {
                        try {
                            callbackRunner.submit(runnable::run);
                        } catch (Exception e) {
                            exceptionHandler.handleException(
                                    "Caught exception when post complete StateFuture.", e);
                        }
                    });
        }
    }

    @Override
    public void callbackFinished() {
        // When a callback ends, as shown in Fig.5 of FLIP-425, at the
        // point of 2,4 and 6, the ref count -1.
        recordContext.release(Runnable::run);
    }
}
