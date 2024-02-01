/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.LambdaUtil;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

/** Helpers for {@link StateObject} related code. */
public class StateUtil {

    private static final Logger LOG = LoggerFactory.getLogger(StateUtil.class);

    private StateUtil() {
        throw new AssertionError();
    }

    /**
     * Returns the size of a state object.
     *
     * @param handle The handle to the retrieved state
     */
    public static long getStateSize(StateObject handle) {
        return handle == null ? 0 : handle.getStateSize();
    }

    /**
     * Iterates through the passed state handles and calls discardState() on each handle that is not
     * null. All occurring exceptions are suppressed and collected until the iteration is over and
     * emitted as a single exception.
     *
     * @param handlesToDiscard State handles to discard. Passed iterable is allowed to deliver null
     *     values.
     * @throws Exception exception that is a collection of all suppressed exceptions that were
     *     caught during iteration
     */
    public static void bestEffortDiscardAllStateObjects(
            Iterable<? extends StateObject> handlesToDiscard) throws Exception {
        LambdaUtil.applyToAllWhileSuppressingExceptions(
                handlesToDiscard, StateObject::discardState);
    }

    public static void discardStateObjectQuietly(StateObject stateObject) {
        if (stateObject == null) {
            return;
        }
        try {
            stateObject.discardState();
        } catch (Exception exception) {
            LOG.warn("Discard {} exception.", stateObject, exception);
        }
    }

    /**
     * Discards the given state future by first trying to cancel it. If this is not possible, then
     * the state object contained in the future is calculated and afterwards discarded.
     *
     * @param stateFuture to be discarded
     * @throws Exception if the discard operation failed
     * @return the size of state before cancellation (if available)
     */
    public static Tuple2<Long, Long> discardStateFuture(Future<? extends StateObject> stateFuture)
            throws Exception {
        long stateSize = 0, checkpointedSize = 0;
        if (null != stateFuture) {
            if (!stateFuture.cancel(true)) {

                try {
                    // We attempt to get a result, in case the future completed before cancellation.
                    if (stateFuture instanceof RunnableFuture<?> && !stateFuture.isDone()) {
                        ((RunnableFuture<?>) stateFuture).run();
                    }
                    StateObject stateObject = stateFuture.get();
                    if (stateObject != null) {
                        stateSize = stateObject.getStateSize();
                        checkpointedSize = getCheckpointedSize(stateObject, stateSize);
                        stateObject.discardState();
                    }

                } catch (Exception ex) {
                    LOG.debug(
                            "Cancelled execution of snapshot future runnable. Cancellation produced the following "
                                    + "exception, which is expected an can be ignored.",
                            ex);
                }
            } else if (stateFuture.isDone()) {
                try {
                    StateObject stateObject = stateFuture.get();
                    stateSize = stateObject.getStateSize();
                    checkpointedSize = getCheckpointedSize(stateObject, stateSize);
                } catch (Exception e) {
                    // ignored
                }
            }
        }
        return Tuple2.of(stateSize, checkpointedSize);
    }

    private static long getCheckpointedSize(StateObject stateObject, long stateSize) {
        return stateObject instanceof CompositeStateHandle
                ? ((CompositeStateHandle) stateObject).getCheckpointedSize()
                : stateSize;
    }

    /**
     * Creates an {@link RuntimeException} that signals that an operation did not get the type of
     * {@link StateObject} that was expected. This can mostly happen when a different {@link
     * StateBackend} from the one that was used for taking a checkpoint/savepoint is used when
     * restoring.
     */
    @SuppressWarnings("unchecked")
    public static RuntimeException unexpectedStateHandleException(
            Class<? extends StateObject> expectedStateHandleClass,
            Class<? extends StateObject> actualStateHandleClass) {
        return unexpectedStateHandleException(
                new Class[] {expectedStateHandleClass}, actualStateHandleClass);
    }

    /**
     * Creates a {@link RuntimeException} that signals that an operation did not get the type of
     * {@link StateObject} that was expected. This can mostly happen when a different {@link
     * StateBackend} from the one that was used for taking a checkpoint/savepoint is used when
     * restoring.
     */
    public static RuntimeException unexpectedStateHandleException(
            Class<? extends StateObject>[] expectedStateHandleClasses,
            Class<? extends StateObject> actualStateHandleClass) {

        return new IllegalStateException(
                "Unexpected state handle type, expected one of: "
                        + Joiner.on(", ").join(expectedStateHandleClasses)
                        + ", but found: "
                        + actualStateHandleClass
                        + ". "
                        + "This can mostly happen when a different StateBackend from the one "
                        + "that was used for taking a checkpoint/savepoint is used when restoring.");
    }
}
