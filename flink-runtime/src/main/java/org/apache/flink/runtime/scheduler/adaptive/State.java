/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * State abstraction of the {@link AdaptiveScheduler}. This interface contains all methods every
 * state implementation must support.
 */
interface State extends LabeledGlobalFailureHandler {

    /**
     * This method is called whenever one transitions out of this state.
     *
     * @param newState newState is the state into which the scheduler transitions
     */
    default void onLeave(Class<? extends State> newState) {}

    /** Cancels the job execution. */
    void cancel();

    /**
     * Suspends the job execution.
     *
     * @param cause cause for the suspension
     */
    void suspend(Throwable cause);

    /**
     * Gets the {@link JobID} of the job. The implementation should avoid to use the {@link
     * State#getJob()} method as it may create the {@link ArchivedExecutionGraph} which is
     * expensive.
     *
     * @return the {@link JobID} of the job
     */
    JobID getJobId();

    /**
     * Gets the current {@link JobStatus}. The returned job status will remain unchanged at least
     * until the scheduler transitions to a different state.
     *
     * @return the current {@link JobStatus}
     */
    JobStatus getJobStatus();

    /**
     * Gets the current {@link ArchivedExecutionGraph}.
     *
     * @return the current {@link ArchivedExecutionGraph}
     */
    ArchivedExecutionGraph getJob();

    /**
     * Gets the logger.
     *
     * @return the logger
     */
    Logger getLogger();

    /**
     * Tries to cast this state into a type of the given clazz.
     *
     * @param clazz clazz describes the target type
     * @param <T> target type
     * @return {@link Optional#of} target type if the underlying type can be cast into clazz;
     *     otherwise {@link Optional#empty()}
     */
    default <T> Optional<T> as(Class<? extends T> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
            return Optional.of(clazz.cast(this));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Tries to run the action if this state is of type clazz.
     *
     * @param clazz clazz describes the target type
     * @param action action to run if this state is of the target type
     * @param debugMessage debugMessage which is printed if this state is not the target type
     * @param <T> target type
     * @param <E> error type
     * @throws E an exception if the action fails
     */
    default <T, E extends Exception> void tryRun(
            Class<? extends T> clazz, ThrowingConsumer<T, E> action, String debugMessage) throws E {
        tryRun(
                clazz,
                x -> {
                    getLogger()
                            .debug(
                                    "Running '{}' in state {}.",
                                    debugMessage,
                                    this.getClass().getSimpleName());
                    ThrowingConsumer.unchecked(action).accept(x);
                },
                logger ->
                        logger.debug(
                                "Cannot run '{}' because the actual state is {} and not {}.",
                                debugMessage,
                                this.getClass().getSimpleName(),
                                clazz.getSimpleName()));
    }

    /**
     * Tries to run the action if this state is of type clazz.
     *
     * @param clazz clazz describes the target type
     * @param action action to run if this state is of the target type
     * @param invalidStateCallback that is called if the state isn't matching the expected one.
     * @param <T> target type
     * @param <E> error type
     * @throws E an exception if the action fails
     */
    default <T, E extends Exception> void tryRun(
            Class<? extends T> clazz,
            ThrowingConsumer<T, E> action,
            Consumer<Logger> invalidStateCallback)
            throws E {
        final Optional<? extends T> asOptional = as(clazz);

        if (asOptional.isPresent()) {
            action.accept(asOptional.get());
        } else {
            invalidStateCallback.accept(getLogger());
        }
    }

    /**
     * Tries to run the action if this state is of type clazz.
     *
     * @param clazz clazz describes the target type
     * @param action action to run if this state is of the target type
     * @param debugMessage debugMessage which is printed if this state is not the target type
     * @param <T> target type
     * @param <V> value type
     * @param <E> error type
     * @return {@link Optional#of} the action result if it is successfully executed; otherwise
     *     {@link Optional#empty()}
     * @throws E an exception if the action fails
     */
    default <T, V, E extends Exception> Optional<V> tryCall(
            Class<? extends T> clazz, FunctionWithException<T, V, E> action, String debugMessage)
            throws E {
        final Optional<? extends T> asOptional = as(clazz);

        if (asOptional.isPresent()) {
            return Optional.of(action.apply(asOptional.get()));
        } else {
            getLogger()
                    .debug(
                            "Cannot run '{}' because the actual state is {} and not {}.",
                            debugMessage,
                            this.getClass().getSimpleName(),
                            clazz.getSimpleName());
            return Optional.empty();
        }
    }
}
