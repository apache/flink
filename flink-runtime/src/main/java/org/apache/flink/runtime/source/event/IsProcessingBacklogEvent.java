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

package org.apache.flink.runtime.source.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/** A source event that notify the source operator of the backlog status. */
public class IsProcessingBacklogEvent implements OperatorEvent {
    private static final long serialVersionUID = 1L;
    private final boolean isProcessingBacklog;

    public IsProcessingBacklogEvent(boolean isProcessingBacklog) {
        this.isProcessingBacklog = isProcessingBacklog;
    }

    public boolean isProcessingBacklog() {
        return isProcessingBacklog;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final IsProcessingBacklogEvent that = (IsProcessingBacklogEvent) o;
        return Objects.equals(isProcessingBacklog, that.isProcessingBacklog);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isProcessingBacklog);
    }

    @Override
    public String toString() {
        return String.format("BacklogEvent (backlog='%s')", isProcessingBacklog);
    }
}
