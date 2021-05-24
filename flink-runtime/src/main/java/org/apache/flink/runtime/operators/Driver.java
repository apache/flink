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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.Function;

/**
 * The interface to be implemented by all drivers that run alone (or as the primary driver) in a
 * task. A driver implements the actual code to perform a batch operation, like <i>map()</i>,
 * <i>reduce()</i>, <i>join()</i>, or <i>coGroup()</i>.
 *
 * @see TaskContext
 * @param <S> The type of stub driven by this driver.
 * @param <OT> The data type of the records produced by this driver.
 */
public interface Driver<S extends Function, OT> {

    void setup(TaskContext<S, OT> context);

    /**
     * Gets the number of inputs that the task has.
     *
     * @return The number of inputs.
     */
    int getNumberOfInputs();

    /**
     * Gets the number of comparators required for this driver.
     *
     * @return The number of comparators required for this driver.
     */
    int getNumberOfDriverComparators();

    /**
     * Gets the class of the stub type that is run by this task. For example, a <tt>MapTask</tt>
     * should return <code>MapFunction.class</code>.
     *
     * @return The class of the stub type run by the task.
     */
    Class<S> getStubType();

    /**
     * This method is called before the user code is opened. An exception thrown by this method
     * signals failure of the task.
     *
     * @throws Exception Exceptions may be forwarded and signal task failure.
     */
    void prepare() throws Exception;

    /**
     * The main operation method of the task. It should call the user code with the data subsets
     * until the input is depleted.
     *
     * @throws Exception Any exception thrown by this method signals task failure. Because
     *     exceptions in the user code typically signal situations where this instance in unable to
     *     proceed, exceptions from the user code should be forwarded.
     */
    void run() throws Exception;

    /**
     * This method is invoked in any case (clean termination and exception) at the end of the tasks
     * operation.
     *
     * @throws Exception Exceptions may be forwarded.
     */
    void cleanup() throws Exception;

    /**
     * This method is invoked when the driver must aborted in mid processing. It is invoked
     * asynchronously by a different thread.
     *
     * @throws Exception Exceptions may be forwarded.
     */
    void cancel() throws Exception;
}
