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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.Watermark;

import java.io.IOException;

/**
 * The {@code SinkWriter} is responsible for writing data.
 *
 * @param <InputT> The type of the sink writer's input
 */
@PublicEvolving
public interface SinkWriter<InputT> extends AutoCloseable {

    /**
     * Adds an element to the writer.
     *
     * @param element The input record
     * @param context The additional information about the input record
     * @throws IOException if fail to add an element.
     */
    void write(InputT element, Context context) throws IOException, InterruptedException;

    /**
     * Called on checkpoint or end of input so that the writer to flush all pending data for
     * at-least-once.
     */
    void flush(boolean endOfInput) throws IOException, InterruptedException;

    /**
     * Adds a watermark to the writer.
     *
     * <p>This method is intended for advanced sinks that propagate watermarks.
     *
     * @param watermark The watermark.
     * @throws IOException if fail to add a watermark.
     */
    default void writeWatermark(Watermark watermark) throws IOException, InterruptedException {}

    /** Context that {@link #write} can use for getting additional data about an input record. */
    @PublicEvolving
    interface Context {

        /** Returns the current event-time watermark. */
        long currentWatermark();

        /**
         * Returns the timestamp of the current input record or {@code null} if the element does not
         * have an assigned timestamp.
         */
        Long timestamp();
    }
}
