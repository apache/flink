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

package org.apache.flink.api.connector.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;

public interface CommittingSink<InputT, CommT, WriterStateT>
        extends StatefulSink<InputT, WriterStateT> {
    /**
     * Create a {@link SinkWriter}.
     *
     * @param context the runtime context.
     * @param states the writer's state.
     * @return A sink writer.
     * @throws IOException if fail to create a writer.
     */
    CommittingSinkWriter<InputT, CommT, WriterStateT> createWriter(
            InitContext context, List<WriterStateT> states) throws IOException;

    /**
     * Creates a {@link Committer}.
     *
     * @return A committer.
     * @throws IOException if fail to create a committer.
     */
    Committer<CommT> createCommitter(Sink.InitContext context) throws IOException;

    /** Returns the serializer of the committable type. */
    SimpleVersionedSerializer<CommT> getCommittableSerializer();
}
