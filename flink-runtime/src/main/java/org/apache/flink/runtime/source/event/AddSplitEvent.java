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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A source event that adds splits to a source reader.
 *
 * @param <SplitT> the type of splits.
 */
public class AddSplitEvent<SplitT> implements OperatorEvent {

    private static final long serialVersionUID = 1L;
    private final int serializerVersion;
    private final ArrayList<byte[]> splits;

    public AddSplitEvent(List<SplitT> splits, SimpleVersionedSerializer<SplitT> splitSerializer)
            throws IOException {
        this.splits = new ArrayList<>(splits.size());
        this.serializerVersion = splitSerializer.getVersion();
        for (SplitT split : splits) {
            this.splits.add(splitSerializer.serialize(split));
        }
    }

    public List<SplitT> splits(SimpleVersionedSerializer<SplitT> splitSerializer)
            throws IOException {
        List<SplitT> result = new ArrayList<>(splits.size());
        for (byte[] serializedSplit : splits) {
            result.add(splitSerializer.deserialize(serializerVersion, serializedSplit));
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("AddSplitEvents[%s]", splits);
    }
}
