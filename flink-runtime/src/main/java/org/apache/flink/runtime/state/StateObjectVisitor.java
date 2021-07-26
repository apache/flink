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
package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.FullyFinishedOperatorState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryChangelogStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.io.Serializable;

/**
 * {@link StateObject} visitor allows recursive traversal and addition of new operations to state
 * object hierarchy. Each type of {@link StateObject} must implement {@link
 * StateObject#accept(StateObjectVisitor) accept} method by calling {@link #visit(StateObject)
 * visitor.visit} thereby choosing a specific method. It is the {@link StateObject} that is expected
 * to pass this visitor to its children.
 */
@Internal
@SuppressWarnings("unused")
public interface StateObjectVisitor<E extends Exception> {

    default void visit(StateObject stateObject) throws E {}

    default <T extends StateObject> void visit(SnapshotResult<T> stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(SubtaskState stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(OperatorSubtaskState stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(TaskStateSnapshot stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(StreamStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default <Info> void visit(AbstractChannelStateHandle<Info> stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default <T extends Serializable> void visit(RetrievableStateHandle<T> stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default <T extends Serializable> void visit(RetrievableStreamStateHandle<T> stateObject)
            throws E {
        visit((StreamStateHandle) stateObject);
    }

    default <T extends StateObject> void visit(ChainedStateHandle<T> stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default <T extends StateObject> void visit(StateObjectCollection<T> stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(ByteStreamStateHandle stateObject) throws E {
        visit((StreamStateHandle) stateObject);
    }

    default void visit(ChangelogStateBackendHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(ChangelogStateBackendHandleImpl stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(ChangelogStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(ChangelogStateHandleStreamImpl stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(CompositeStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(DirectoryKeyedStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(DirectoryStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(FileStateHandle stateObject) throws E {
        visit((StreamStateHandle) stateObject);
    }

    default void visit(FullyFinishedOperatorState stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(InMemoryChangelogStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(IncrementalKeyedStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(IncrementalLocalKeyedStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(IncrementalRemoteKeyedStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(InputChannelStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(KeyGroupsSavepointStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(KeyGroupsStateHandle stateObject) throws E {
        visit((StreamStateHandle) stateObject);
    }

    default void visit(KeyedStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(OperatorState stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(OperatorStateHandle stateObject) throws E {
        visit((StreamStateHandle) stateObject);
    }

    default void visit(OperatorStreamStateHandle stateObject) throws E {
        visit((OperatorStateHandle) stateObject);
    }

    default void visit(PlaceholderStreamStateHandle stateObject) throws E {
        visit((StreamStateHandle) stateObject);
    }

    default void visit(RelativeFileStateHandle stateObject) throws E {
        visit((FileStateHandle) stateObject);
    }

    default void visit(ResultSubpartitionStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(SavepointKeyedStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }

    @SuppressWarnings("deprecation")
    default void visit(TaskState stateObject) throws E {
        visit((StateObject) stateObject);
    }

    default void visit(ShareableStateHandle stateObject) throws E {
        visit((StateObject) stateObject);
    }
}
