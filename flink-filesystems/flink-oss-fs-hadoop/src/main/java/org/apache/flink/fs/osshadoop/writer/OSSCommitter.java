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

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.osshadoop.OSSAccessor;

import com.aliyun.oss.model.PartETag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Data object to commit an OSS MultiPartUpload. */
public class OSSCommitter implements RecoverableFsDataOutputStream.Committer {
    private static final Logger LOG = LoggerFactory.getLogger(OSSCommitter.class);

    private OSSAccessor ossAccessor;
    private String objectName;
    private String uploadId;
    private List<PartETag> partETags;
    private long totalLength;

    public OSSCommitter(
            OSSAccessor ossAccessor,
            String objectName,
            String uploadId,
            List<PartETag> partETags,
            long totalLength) {
        this.ossAccessor = checkNotNull(ossAccessor);
        this.objectName = checkNotNull(objectName);
        this.uploadId = checkNotNull(uploadId);
        this.partETags = checkNotNull(partETags);
        this.totalLength = totalLength;
    }

    @Override
    public void commit() throws IOException {
        if (totalLength > 0L) {
            LOG.info("Committing {} with multi-part upload ID {}", objectName, uploadId);
            ossAccessor.completeMultipartUpload(objectName, uploadId, partETags);
        } else {
            LOG.debug("No data to commit for file: {}", objectName);
        }
    }

    @Override
    public void commitAfterRecovery() throws IOException {
        if (totalLength > 0L) {
            try {
                LOG.info(
                        "Trying to commit after recovery {} with multi-part upload ID {}",
                        objectName,
                        uploadId);
                ossAccessor.completeMultipartUpload(objectName, uploadId, partETags);
            } catch (Exception e) {
                LOG.info(
                        "Failed to commit after recovery {} with multi-part upload ID {}. "
                                + "exception {}",
                        objectName,
                        uploadId,
                        e);
            }
        } else {
            LOG.debug("No data to commit for file: {}", objectName);
        }
    }

    @Override
    public RecoverableWriter.CommitRecoverable getRecoverable() {
        return new OSSRecoverable(uploadId, objectName, partETags, null, totalLength, 0);
    }
}
