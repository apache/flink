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

package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** Request of file compacting for {@link FileSink}. */
@Internal
public class CompactorRequest implements Serializable {
    private final String bucketId;
    private final List<FileSinkCommittable> committableToCompact;
    private final List<FileSinkCommittable> committableToPassthrough;

    public CompactorRequest(String bucketId) {
        this.bucketId = bucketId;
        this.committableToCompact = new ArrayList<>();
        this.committableToPassthrough = new ArrayList<>();
    }

    public CompactorRequest(
            String bucketId,
            List<FileSinkCommittable> committableToCompact,
            List<FileSinkCommittable> committableToPassthrough) {
        this.bucketId = bucketId;
        this.committableToCompact = committableToCompact;
        this.committableToPassthrough = committableToPassthrough;
    }

    public void addToCompact(FileSinkCommittable committable) {
        checkState(committable.hasPendingFile());
        committableToCompact.add(committable);
    }

    public void addToPassthrough(FileSinkCommittable committable) {
        committableToPassthrough.add(committable);
    }

    public String getBucketId() {
        return bucketId;
    }

    public List<FileSinkCommittable> getCommittableToCompact() {
        return committableToCompact;
    }

    public List<FileSinkCommittable> getCommittableToPassthrough() {
        return committableToPassthrough;
    }
}
