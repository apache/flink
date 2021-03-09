/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Optional;

/**
 * A {@link StateObject} that represents state that was written to a stream. The data can be read
 * back via {@link #openInputStream()}.
 */
public interface StreamStateHandle extends StateObject {

    /**
     * Returns an {@link FSDataInputStream} that can be used to read back the data that was
     * previously written to the stream.
     */
    FSDataInputStream openInputStream() throws IOException;

    /** @return Content of this handle as bytes array if it is already in memory. */
    Optional<byte[]> asBytesIfInMemory();
}
