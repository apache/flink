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

package org.apache.flink.runtime.state.filemerging;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/** An empty {@link SegmentFileStateHandle} that is only used as a placeholder. */
public class EmptySegmentFileStateHandle extends SegmentFileStateHandle {
    private static final long serialVersionUID = 1L;

    public static final EmptySegmentFileStateHandle INSTANCE =
            new EmptySegmentFileStateHandle(
                    new Path("empty"), 0, 0, CheckpointedStateScope.EXCLUSIVE);

    private EmptySegmentFileStateHandle(
            Path filePath, long startPos, long stateSize, CheckpointedStateScope scope) {
        super(filePath, startPos, stateSize, scope);
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        throw new UnsupportedEncodingException(
                "Cannot open input stream from an EmptySegmentFileStateHandle.");
    }
}
