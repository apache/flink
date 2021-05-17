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

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.Internal;

import java.util.function.Consumer;

/**
 * {@link StreamStatus#IDLE} requires that no records nor watermarks travel through the branch. In
 * order to keep the older behaviour that records could've been generated down the pipeline even
 * though the sources were idle we go through a short ACTIVE/IDLE loop. This is a helper class that
 * lets you easily flip the status around a code block.
 */
@Internal
public final class AnnouncedStatus {
    private StreamStatus currentStatus;

    public AnnouncedStatus(StreamStatus currentStatus) {
        this.currentStatus = currentStatus;
    }

    public StreamStatus getCurrentStatus() {
        return currentStatus;
    }

    public void setCurrentStatus(StreamStatus currentStatus) {
        this.currentStatus = currentStatus;
    }

    /**
     * Makes sure that the last emitted StreamStatus was ACTIVE.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * try (AutoCloseable ignored = announcedStatus.ensureActive(this::writeStreamStatus)) {
     *     serializationDelegate.setInstance(record);
     *     recordWriter.emit(serializationDelegate);
     * } catch (Exception e) {
     *     throw new RuntimeException(e.getMessage(), e);
     * }
     * }</pre>
     *
     * @param statusConsumer a consumer which sends the status downstream
     */
    public AutoCloseable ensureActive(Consumer<StreamStatus> statusConsumer) {
        if (currentStatus.isIdle()) {
            statusConsumer.accept(StreamStatus.ACTIVE);
            return () -> statusConsumer.accept(StreamStatus.IDLE);
        }
        return () -> {};
    }
}
