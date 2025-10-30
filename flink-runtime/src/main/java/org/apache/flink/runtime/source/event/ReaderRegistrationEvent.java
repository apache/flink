/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.event;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SupportsSplitReassignmentOnRecovery;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The SourceOperator should always send the ReaderRegistrationEvent with the
 * `reportedSplitsOnRegistration` list. But it will not add the splits to readers if {@link
 * SupportsSplitReassignmentOnRecovery} is implemented.
 */
public class ReaderRegistrationEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final int subtaskId;
    private final String location;
    private final ArrayList<byte[]> splits;

    public ReaderRegistrationEvent(int subtaskId, String location) {
        this.subtaskId = subtaskId;
        this.location = location;
        this.splits = new ArrayList<>();
    }

    ReaderRegistrationEvent(int subtaskId, String location, ArrayList<byte[]> splits) {
        this.subtaskId = subtaskId;
        this.location = location;
        this.splits = splits;
    }

    public static <SplitT extends SourceSplit>
            ReaderRegistrationEvent createReaderRegistrationEvent(
                    int subtaskId,
                    String location,
                    List<SplitT> splits,
                    SimpleVersionedSerializer<SplitT> splitSerializer)
                    throws IOException {
        ArrayList<byte[]> result = new ArrayList<>();
        for (SplitT split : splits) {
            result.add(splitSerializer.serialize(split));
        }
        return new ReaderRegistrationEvent(subtaskId, location, result);
    }

    public <SplitT extends SourceSplit> List<SplitT> splits(
            SimpleVersionedSerializer<SplitT> splitSerializer) throws IOException {
        if (splits.isEmpty()) {
            return Collections.emptyList();
        }
        List<SplitT> result = new ArrayList<>(splits.size());
        for (byte[] serializedSplit : splits) {
            result.add(splitSerializer.deserialize(splitSerializer.getVersion(), serializedSplit));
        }
        return result;
    }

    public int subtaskId() {
        return subtaskId;
    }

    public String location() {
        return location;
    }

    @Override
    public String toString() {
        return String.format(
                "ReaderRegistrationEvent[subtaskId = %d, location = %s)", subtaskId, location);
    }
}
