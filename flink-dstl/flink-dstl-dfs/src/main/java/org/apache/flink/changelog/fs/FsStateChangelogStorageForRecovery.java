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

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleStreamHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;

import javax.annotation.concurrent.ThreadSafe;

/** Filesystem-based implementation of {@link StateChangelogStorage} just for recovery. */
@Experimental
@ThreadSafe
public class FsStateChangelogStorageForRecovery
        implements StateChangelogStorageView<ChangelogStateHandleStreamImpl> {

    private final ChangelogStreamHandleReader changelogStreamHandleReader;

    public FsStateChangelogStorageForRecovery(
            ChangelogStreamHandleReader changelogStreamHandleReader) {
        this.changelogStreamHandleReader = changelogStreamHandleReader;
    }

    @Override
    public StateChangelogHandleReader<ChangelogStateHandleStreamImpl> createReader() {
        return new StateChangelogHandleStreamHandleReader(
                new StateChangeIteratorImpl(changelogStreamHandleReader));
    }

    @Override
    public void close() throws Exception {
        if (changelogStreamHandleReader != null) {
            changelogStreamHandleReader.close();
        }
    }
}
