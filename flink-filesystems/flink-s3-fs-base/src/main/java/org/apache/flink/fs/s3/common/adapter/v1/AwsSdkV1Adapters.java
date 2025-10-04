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

package org.apache.flink.fs.s3.common.adapter.v1;

import org.apache.flink.annotation.Internal;
import org.apache.flink.fs.s3.common.model.FlinkCompleteMultipartUploadResult;
import org.apache.flink.fs.s3.common.model.FlinkObjectMetadata;
import org.apache.flink.fs.s3.common.model.FlinkPartETag;
import org.apache.flink.fs.s3.common.model.FlinkPutObjectResult;
import org.apache.flink.fs.s3.common.model.FlinkUploadPartResult;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartResult;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapters to convert between Flink SDK-agnostic types and AWS SDK v1 types.
 *
 * <p>This class provides static utility methods for converting between Flink's SDK-agnostic model
 * classes and AWS SDK v1 model classes. These adapters enable the S3 filesystem implementation to
 * use AWS SDK v1 (e.g., for Presto-based implementations) while working with SDK-agnostic
 * interfaces.
 */
@Internal
public final class AwsSdkV1Adapters {

    private AwsSdkV1Adapters() {
        // Utility class, no instantiation
    }

    // ========== PartETag Conversions ==========

    /**
     * Converts an AWS SDK v1 PartETag to a Flink PartETag.
     *
     * @param partETag the AWS SDK v1 PartETag
     * @return the Flink PartETag
     */
    public static FlinkPartETag toFlinkPartETag(PartETag partETag) {
        return new FlinkPartETag(partETag.getPartNumber(), partETag.getETag());
    }

    /**
     * Converts a Flink PartETag to an AWS SDK v1 PartETag.
     *
     * @param flinkPartETag the Flink PartETag
     * @return the AWS SDK v1 PartETag
     */
    public static PartETag toAwsPartETag(FlinkPartETag flinkPartETag) {
        return new PartETag(flinkPartETag.getPartNumber(), flinkPartETag.getETag());
    }

    /**
     * Converts a list of AWS SDK v1 PartETags to Flink PartETags.
     *
     * @param partETags the list of AWS SDK v1 PartETags
     * @return the list of Flink PartETags
     */
    public static List<FlinkPartETag> toFlinkPartETags(List<PartETag> partETags) {
        return partETags.stream()
                .map(AwsSdkV1Adapters::toFlinkPartETag)
                .collect(Collectors.toList());
    }

    /**
     * Converts a list of Flink PartETags to AWS SDK v1 PartETags.
     *
     * @param flinkPartETags the list of Flink PartETags
     * @return the list of AWS SDK v1 PartETags
     */
    public static List<PartETag> toAwsPartETags(List<FlinkPartETag> flinkPartETags) {
        return flinkPartETags.stream()
                .map(AwsSdkV1Adapters::toAwsPartETag)
                .collect(Collectors.toList());
    }

    // ========== UploadPartResult Conversions ==========

    /**
     * Converts an AWS SDK v1 UploadPartResult to a Flink UploadPartResult.
     *
     * @param result the AWS SDK v1 UploadPartResult
     * @return the Flink UploadPartResult
     */
    public static FlinkUploadPartResult toFlinkUploadPartResult(UploadPartResult result) {
        return new FlinkUploadPartResult(result.getPartNumber(), result.getETag());
    }

    // ========== PutObjectResult Conversions ==========

    /**
     * Converts an AWS SDK v1 PutObjectResult to a Flink PutObjectResult.
     *
     * @param result the AWS SDK v1 PutObjectResult
     * @return the Flink PutObjectResult
     */
    public static FlinkPutObjectResult toFlinkPutObjectResult(PutObjectResult result) {
        return new FlinkPutObjectResult(result.getETag(), result.getVersionId());
    }

    // ========== CompleteMultipartUploadResult Conversions ==========

    /**
     * Converts an AWS SDK v1 CompleteMultipartUploadResult to a Flink
     * CompleteMultipartUploadResult.
     *
     * @param result the AWS SDK v1 CompleteMultipartUploadResult
     * @return the Flink CompleteMultipartUploadResult
     */
    public static FlinkCompleteMultipartUploadResult toFlinkCompleteMultipartUploadResult(
            CompleteMultipartUploadResult result) {
        return FlinkCompleteMultipartUploadResult.builder()
                .bucket(result.getBucketName())
                .key(result.getKey())
                .eTag(result.getETag())
                .location(result.getLocation())
                .versionId(result.getVersionId())
                .build();
    }

    // ========== ObjectMetadata Conversions ==========

    /**
     * Converts an AWS SDK v1 ObjectMetadata to a Flink ObjectMetadata.
     *
     * @param metadata the AWS SDK v1 ObjectMetadata
     * @return the Flink ObjectMetadata
     */
    public static FlinkObjectMetadata toFlinkObjectMetadata(ObjectMetadata metadata) {
        return FlinkObjectMetadata.builder()
                .contentLength(metadata.getContentLength())
                .contentType(metadata.getContentType())
                .eTag(metadata.getETag())
                .lastModified(metadata.getLastModified())
                .userMetadata(metadata.getUserMetadata())
                .build();
    }
}
