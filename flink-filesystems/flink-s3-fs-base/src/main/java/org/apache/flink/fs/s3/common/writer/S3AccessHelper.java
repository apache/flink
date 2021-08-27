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

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartResult;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An interface that abstracts away the Multi-Part Upload (MPU) functionality offered by S3, from
 * the specific implementation of the file system. This is needed so that we can accommodate both
 * Hadoop S3 and Presto.
 *
 * <p>Multipart uploads are convenient for large object. These will be uploaded in multiple parts
 * and the mutli-part upload is the equivalent of a transaction, where the upload with all its parts
 * will be either committed or discarded.
 */
@Internal
public interface S3AccessHelper {

    /**
     * Initializes a Multi-Part Upload.
     *
     * @param key the key whose value we want to upload in parts.
     * @return The id of the initiated Multi-Part Upload which will be used during the uploading of
     *     the parts.
     * @throws IOException
     */
    String startMultiPartUpload(String key) throws IOException;

    /**
     * Uploads a part and associates it with the MPU with the provided {@code uploadId}.
     *
     * @param key the key this MPU is associated with.
     * @param uploadId the id of the MPU.
     * @param partNumber the number of the part being uploaded (has to be in [1 ... 10000]).
     * @param inputFile the (local) file holding the part to be uploaded.
     * @param length the length of the part.
     * @return The {@link UploadPartResult result} of the attempt to upload the part.
     * @throws IOException
     */
    UploadPartResult uploadPart(
            String key, String uploadId, int partNumber, File inputFile, long length)
            throws IOException;

    /**
     * Uploads an object to S3. Contrary to the {@link #uploadPart(String, String, int, File, long)}
     * method, this object is not going to be associated to any MPU and, as such, it is not subject
     * to the garbage collection policies specified for your S3 bucket.
     *
     * @param key the key used to identify this part.
     * @param inputFile the (local) file holding the data to be uploaded.
     * @return The {@link PutObjectResult result} of the attempt to stage the incomplete part.
     * @throws IOException
     */
    PutObjectResult putObject(String key, File inputFile) throws IOException;

    /**
     * Finalizes a Multi-Part Upload.
     *
     * @param key the key identifying the object we finished uploading.
     * @param uploadId the id of the MPU.
     * @param partETags the list of {@link PartETag ETags} associated with this MPU.
     * @param length the size of the uploaded object.
     * @param errorCount a counter that will be used to count any failed attempts to commit the MPU.
     * @return The {@link CompleteMultipartUploadResult result} of the attempt to finalize the MPU.
     * @throws IOException
     */
    CompleteMultipartUploadResult commitMultiPartUpload(
            String key,
            String uploadId,
            List<PartETag> partETags,
            long length,
            AtomicInteger errorCount)
            throws IOException;

    /**
     * Deletes the object associated with the provided key.
     *
     * @param key The key to be deleted.
     * @return {@code true} if the resources were successfully freed, {@code false} otherwise (e.g.
     *     the file to be deleted was not there).
     * @throws IOException
     */
    boolean deleteObject(String key) throws IOException;

    /**
     * Gets the object associated with the provided {@code key} from S3 and puts it in the provided
     * {@code targetLocation}.
     *
     * @param key the key of the object to fetch.
     * @param targetLocation the file to read the object to.
     * @return The number of bytes read.
     * @throws IOException
     */
    long getObject(String key, File targetLocation) throws IOException;

    /**
     * Fetches the metadata associated with a given key on S3.
     *
     * @param key the key.
     * @return The associated {@link ObjectMetadata}.
     * @throws IOException
     */
    ObjectMetadata getObjectMetadata(String key) throws IOException;
}
