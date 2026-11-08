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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The part number and ETag of an uploaded part of a multipart upload.
 *
 * <p>The writer classes are bundled into the presto plugin as well, which ships no AWS SDK v2, so
 * they use this type instead of the SDK's {@code CompletedPart}.
 */
@Internal
public final class PartETag {

    private final int partNumber;

    private final String eTag;

    public PartETag(int partNumber, String eTag) {
        this.partNumber = partNumber;
        this.eTag = checkNotNull(eTag);
    }

    public int getPartNumber() {
        return partNumber;
    }

    public String getETag() {
        return eTag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PartETag that = (PartETag) o;
        return partNumber == that.partNumber && eTag.equals(that.eTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partNumber, eTag);
    }

    @Override
    public String toString() {
        return partNumber + "=" + eTag;
    }
}
