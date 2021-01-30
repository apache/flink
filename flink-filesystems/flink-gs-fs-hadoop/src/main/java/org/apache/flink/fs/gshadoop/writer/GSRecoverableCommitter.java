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

package org.apache.flink.fs.gshadoop.writer;

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

/** A committer for a recoverable upload to Google Storage. */
class GSRecoverableCommitter implements RecoverableFsDataOutputStream.Committer {

    private final GSRecoverableOptions options;

    private final GSRecoverable recoverable;

    GSRecoverableCommitter(GSRecoverableOptions options, GSRecoverable recoverable) {
        this.options = Preconditions.checkNotNull(options);
        this.recoverable = Preconditions.checkNotNull(recoverable);
    }

    private void deleteTempBlob() {
        // this call returns a boolean which would be false to indicate the referenced blob
        // doesn't exist; we disregard that result here, as either true or false means that
        // the temp blob is gone
        options.helper.getStorage().delete(recoverable.plan.tempBlobId);
    }

    @Override
    public void commit() {
        // copy from the temp blob to the final one
        options.helper.getStorage().copy(recoverable.plan.tempBlobId, recoverable.plan.finalBlobId);

        // delete the temp blob
        deleteTempBlob();
    }

    @Override
    public void commitAfterRecovery() {
        // is the final blob in place?
        if (options.helper.getStorage().exists(recoverable.plan.finalBlobId)) {
            // yes, so just make sure that the temp blob is deleted
            deleteTempBlob();
        } else {
            // no, so do the full commit
            commit();
        }
    }

    @Override
    public RecoverableWriter.CommitRecoverable getRecoverable() {
        return recoverable;
    }
}
