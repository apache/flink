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

package org.apache.flink.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;

/** The base interface of runner which is responsible for the execution of Python functions. */
@Internal
public interface PythonFunctionRunner {

    /**
     * Prepares the Python function runner, such as preparing the Python execution environment, etc.
     */
    void open(PythonConfig config) throws Exception;

    /** Tear-down the Python function runner. */
    void close() throws Exception;

    /**
     * Executes the Python function with the input byte array.
     *
     * @param data the byte array data.
     */
    void process(byte[] data) throws Exception;

    /** Send the triggered timer to the Python function. */
    void processTimer(byte[] timerData) throws Exception;

    /**
     * Retrieves the Python function result.
     *
     * @return the head of he Python function result buffer, or {@code null} if the result buffer is
     *     empty. f0 means the byte array buffer which stores the Python function result. f1 means
     *     the length of the Python function result byte array.
     */
    Tuple2<byte[], Integer> pollResult() throws Exception;

    /**
     * Retrieves the Python function result, waiting if necessary until an element becomes
     * available.
     *
     * @return the head of he Python function result buffer. f0 means the byte array buffer which
     *     stores the Python function result. f1 means the length of the Python function result byte
     *     array.
     */
    Tuple2<byte[], Integer> takeResult() throws Exception;

    /**
     * Forces to finish the processing of the current bundle of elements. It will flush the data
     * cached in the data buffer for processing and retrieves the state mutations (if exists) made
     * by the Python function. The call blocks until all of the outputs produced by this bundle have
     * been received.
     */
    void flush() throws Exception;
}
