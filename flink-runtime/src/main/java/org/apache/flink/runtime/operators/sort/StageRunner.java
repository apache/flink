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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.util.MutableObjectIterator;

/**
 * An interface for different stages of the sorting process. Different stages can communicate via
 * the {@link StageMessageDispatcher}.
 */
public interface StageRunner extends AutoCloseable {
    /** Starts the stage. */
    void start();

    /** A marker interface for sending messages to different stages. */
    enum SortStage {
        READ,
        SORT,
        SPILL
    }

    /**
     * A dispatcher for inter-stage communication. It allows for returning a result to a {@link
     * Sorter} via {@link StageMessageDispatcher#sendResult(MutableObjectIterator)}
     */
    interface StageMessageDispatcher<E> extends AutoCloseable {
        /** Sends a message to the given stage. */
        void send(SortStage stage, CircularElement<E> element);

        /**
         * Retrieves and removes the head of the given queue, waiting if necessary until an element
         * becomes available.
         *
         * @return the head of the queue
         */
        CircularElement<E> take(SortStage stage) throws InterruptedException;

        /**
         * Retrieves and removes the head of the given stage queue, or returns {@code null} if the
         * queue is empty.
         *
         * @return the head of the queue, or {@code null} if the queue is empty
         */
        CircularElement<E> poll(SortStage stage);

        /** Sends a result to the corresponding {@link Sorter}. */
        void sendResult(MutableObjectIterator<E> result);
    }
}
