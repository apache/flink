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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.DataInputStream;
import java.io.IOException;

import static org.apache.flink.changelog.fs.ChangelogStreamWrapper.wrapAndSeek;

/** Changelog handle reader to use by {@link StateChangeIteratorImpl}. */
@Internal
interface ChangelogStreamHandleReader extends AutoCloseable {

    DataInputStream openAndSeek(StreamStateHandle handle, Long offset) throws IOException;

    @Override
    default void close() throws Exception {}

    ChangelogStreamHandleReader DIRECT_READER =
            (handle, offset) -> wrapAndSeek(handle.openInputStream(), offset);
}
