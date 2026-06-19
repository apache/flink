/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import javax.annotation.Nullable;

/**
 * An invokable part of the task.
 *
 * <p>The TaskManager first calls the {@link #restore} method when executing a task. If the call
 * succeeds and the task isn't cancelled then TM proceeds to {@link #invoke()}. All operations of
 * the task happen in these two methods (setting up input output stream readers and writers as well
 * as the task's core operation).
 *
 * <p>After that, {@link #cleanUp(Throwable)} is called (regardless of an failures or cancellations
 * during the above calls).
 *
 * <p>Implementations must have a constructor with a single argument of type {@link
 * org.apache.flink.runtime.execution.Environment}.
 *
 * <p><i>Developer note: While constructors cannot be enforced at compile time, we did not yet
 * venture on the endeavor of introducing factories (it is only an internal API after all, and with
 * Java 8, one can use {@code Class::new} almost like a factory lambda.</i>
 *
 * @see CheckpointableTask
 * @see CoordinatedTask
 * @see AbstractInvokable
 */
@Internal
public interface TaskInvokable {

    /**
     * Starts the execution.
     *
     * <p>This method is called by the task manager when the actual execution of the task starts.
     *
     * <p>All resources should be cleaned up by calling {@link #cleanUp(Throwable)} after the method
     * returns.
     */
    void invoke() throws Exception;

    /**
     * This method can be called before {@link #invoke()} to restore an invokable object for the
     * last valid state, if it has it.
     *
     * <p>If {@link #invoke()} is not called after this method for some reason (e.g. task
     * cancellation); then all resources should be cleaned up by calling {@link #cleanUp(Throwable)}
     * ()} after the method returns.
     */
    void restore() throws Exception;

    /**
     * Cleanup any resources used in {@link #invoke()} OR {@link #restore()}. This method must be
     * called regardless whether the aforementioned calls succeeded or failed.
     *
     * @param throwable iff failure happened during the execution of {@link #restore()} or {@link
     *     #invoke()}, null otherwise.
     *     <p>ATTENTION: {@link org.apache.flink.runtime.execution.CancelTaskException
     *     CancelTaskException} should not be treated as a failure.
     */
    void cleanUp(@Nullable Throwable throwable) throws Exception;

    /**
     * This method is called when a task is canceled either as a result of a user abort or an
     * execution failure. It can be overwritten to respond to shut down the user code properly.
     */
    void cancel() throws Exception;

    /**
     * @return true if blocking input such as {@link InputGate#getNext()} is used (as opposed to
     *     {@link InputGate#pollNext()}. To be removed together with the DataSet API.
     */
    boolean isUsingNonBlockingInput();

    /**
     * Checks whether the task should be interrupted during cancellation and if so, execute the
     * specified {@code Runnable interruptAction}.
     *
     * @param toInterrupt
     * @param taskName optional taskName to log stack trace
     * @param timeout optional timeout to log stack trace
     */
    void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout);
}
