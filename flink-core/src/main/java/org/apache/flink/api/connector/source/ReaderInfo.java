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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A container class hosting the information of a {@link SourceReader}.
 *
 * <p>The {@code reportedSplitsOnRegistration} can only be provided when the source implements
 * {@link SupportsSplitReassignmentOnRecovery}.
 */
@Public
public final class ReaderInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int subtaskId;
    private final String location;
    private final List<SourceSplit> reportedSplitsOnRegistration;

    public ReaderInfo(int subtaskId, String location) {
        this(subtaskId, location, Collections.emptyList());
    }

    ReaderInfo(int subtaskId, String location, List<SourceSplit> splits) {
        this.subtaskId = subtaskId;
        this.location = location;
        this.reportedSplitsOnRegistration = splits;
    }

    @SuppressWarnings("unchecked")
    public static <SplitT extends SourceSplit> ReaderInfo createReaderInfo(
            int subtaskId, String location, List<SplitT> splits) {
        return new ReaderInfo(subtaskId, location, (List<SourceSplit>) splits);
    }

    @SuppressWarnings("unchecked")
    public <SplitT extends SourceSplit> List<SplitT> getReportedSplitsOnRegistration() {
        return (List<SplitT>) reportedSplitsOnRegistration;
    }

    /**
     * @return the ID of the subtask that runs the source reader.
     */
    public int getSubtaskId() {
        return subtaskId;
    }

    /**
     * @return the location of the subtask that runs this source reader.
     */
    public String getLocation() {
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ReaderInfo)) {
            return false;
        }
        ReaderInfo that = (ReaderInfo) o;
        return subtaskId == that.subtaskId
                && Objects.equals(location, that.location)
                && Objects.equals(reportedSplitsOnRegistration, that.reportedSplitsOnRegistration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtaskId, location, reportedSplitsOnRegistration);
    }
}
