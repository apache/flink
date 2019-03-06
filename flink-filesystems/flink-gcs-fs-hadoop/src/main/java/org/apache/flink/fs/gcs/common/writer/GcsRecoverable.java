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

package org.apache.flink.fs.gcs.common.writer;

import com.google.common.base.MoreObjects;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data object to recover an GCS for a recoverable output stream.
 */
public class GcsRecoverable implements RecoverableWriter.ResumeRecoverable {
    private static final Logger LOG = LoggerFactory.getLogger(GcsRecoverable.class);

    private final String bucketName, objectName;
    private final int pos;

    public GcsRecoverable(Path gcsFullPath) {
        this.bucketName = gcsFullPath.toUri().getAuthority();
        this.objectName = gcsFullPath.toUri().getPath().substring(1);
        this.pos = 0;

        LOG.info("Deconstructed the bucket {} and object {}", this.bucketName, this.objectName);
    }

    public GcsRecoverable(GcsRecoverable oldRecoverable, int pos) {
        this.bucketName = oldRecoverable.getBucketName();
        this.objectName = oldRecoverable.getObjectName();
        this.pos = pos;

        LOG.info("Deconstructed the bucket {} and object {} at position {}", this.bucketName, this.objectName, this.pos);
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectName() {
        return objectName;
    }

    public int getPos() {
        return this.pos;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("bucketName", bucketName)
                .add("objectName", objectName)
                .add("pos", pos)
                .toString();
    }
}
