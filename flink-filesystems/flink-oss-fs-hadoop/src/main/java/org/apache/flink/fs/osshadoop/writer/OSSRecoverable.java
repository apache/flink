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

package org.apache.flink.fs.osshadoop.writer;

import org.apache.flink.core.fs.RecoverableWriter;

import com.aliyun.oss.model.PartETag;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Data object to recover an OSS MultiPartUpload for a recoverable output stream. */
public class OSSRecoverable implements RecoverableWriter.ResumeRecoverable {
    private final String uploadId;

    private final String objectName;

    private final List<PartETag> partETags;

    @Nullable private final String lastPartObject;

    private long numBytesInParts;

    private long lastPartObjectLength;

    public OSSRecoverable(
            String uploadId,
            String objectName,
            List<PartETag> partETags,
            String lastPartObject,
            long numBytesInParts,
            long lastPartObjectLength) {
        this.uploadId = uploadId;
        this.objectName = objectName;
        this.partETags = new ArrayList<>(partETags);
        this.lastPartObject = lastPartObject;
        this.numBytesInParts = numBytesInParts;
        this.lastPartObjectLength = lastPartObjectLength;
    }

    public String getUploadId() {
        return uploadId;
    }

    public String getObjectName() {
        return objectName;
    }

    public List<PartETag> getPartETags() {
        return partETags;
    }

    @Nullable
    public String getLastPartObject() {
        return lastPartObject;
    }

    public long getNumBytesInParts() {
        return numBytesInParts;
    }

    public long getLastPartObjectLength() {
        return lastPartObjectLength;
    }
}
