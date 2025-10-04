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

package org.apache.flink.fs.s3.common.adapter.v2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.fs.s3.common.model.FlinkCompleteMultipartUploadResult;
import org.apache.flink.fs.s3.common.model.FlinkObjectMetadata;
import org.apache.flink.fs.s3.common.model.FlinkPartETag;
import org.apache.flink.fs.s3.common.model.FlinkPutObjectResult;
import org.apache.flink.fs.s3.common.model.FlinkUploadPartResult;

import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapters to convert between Flink SDK-agnostic types and AWS SDK v2 types.
 *
 * <p>This class provides static utility methods for converting between Flink's SDK-agnostic model
 * classes and AWS SDK v2 model classes. These adapters enable the S3 filesystem implementation to
 * use AWS SDK v2 (e.g., for Hadoop 3.4+ based implementations) while working with SDK-agnostic
 * interfaces.
 */
@Internal
public final class AwsSdkV2Adapters {

    private AwsSdkV2Adapters() {
        // Utility class, no instantiation
    }

    // ========== CompletedPart Conversions ==========

    /**
     * Converts an AWS SDK v2 CompletedPart to a Flink PartETag.
     *
     * @param completedPart the AWS SDK v2 CompletedPart
     * @return the Flink PartETag
     */
    public static FlinkPartETag toFlinkPartETag(CompletedPart completedPart) {
        return new FlinkPartETag(completedPart.partNumber(), completedPart.eTag());
    }

    /**
     * Converts a Flink PartETag to an AWS SDK v2 CompletedPart.
     *
     * @param flinkPartETag the Flink PartETag
     * @return the AWS SDK v2 CompletedPart
     */
    public static CompletedPart toAwsCompletedPart(FlinkPartETag flinkPartETag) {
        return CompletedPart.builder()
                .partNumber(flinkPartETag.getPartNumber())
                .eTag(flinkPartETag.getETag())
                .build();
    }

    /**
     * Converts a list of AWS SDK v2 CompletedParts to Flink PartETags.
     *
     * @param completedParts the list of AWS SDK v2 CompletedParts
     * @return the list of Flink PartETags
     */
    public static List<FlinkPartETag> toFlinkPartETags(List<CompletedPart> completedParts) {
        return completedParts.stream()
                .map(AwsSdkV2Adapters::toFlinkPartETag)
                .collect(Collectors.toList());
    }

    /**
     * Converts a list of Flink PartETags to AWS SDK v2 CompletedParts.
     *
     * @param flinkPartETags the list of Flink PartETags
     * @return the list of AWS SDK v2 CompletedParts
     */
    public static List<CompletedPart> toAwsCompletedParts(List<FlinkPartETag> flinkPartETags) {
        return flinkPartETags.stream()
                .map(AwsSdkV2Adapters::toAwsCompletedPart)
                .collect(Collectors.toList());
    }

    /**
     * Creates an AWS SDK v2 CompletedMultipartUpload from a list of Flink PartETags.
     *
     * @param flinkPartETags the list of Flink PartETags
     * @return the AWS SDK v2 CompletedMultipartUpload
     */
    public static CompletedMultipartUpload toCompletedMultipartUpload(
            List<FlinkPartETag> flinkPartETags) {
        return CompletedMultipartUpload.builder()
                .parts(toAwsCompletedParts(flinkPartETags))
                .build();
    }

    // ========== UploadPartResponse Conversions ==========

    /**
     * Converts an AWS SDK v2 UploadPartResponse to a Flink UploadPartResult.
     *
     * <p>Note: AWS SDK v2 does not return the part number in the response, so the part number must
     * be tracked separately by the caller and provided here.
     *
     * @param response the AWS SDK v2 UploadPartResponse
     * @param partNumber the part number (tracked by caller)
     * @return the Flink UploadPartResult
     */
    public static FlinkUploadPartResult toFlinkUploadPartResult(
            UploadPartResponse response, int partNumber) {
        return new FlinkUploadPartResult(partNumber, response.eTag());
    }

    // ========== PutObjectResponse Conversions ==========

    /**
     * Converts an AWS SDK v2 PutObjectResponse to a Flink PutObjectResult.
     *
     * @param response the AWS SDK v2 PutObjectResponse
     * @return the Flink PutObjectResult
     */
    public static FlinkPutObjectResult toFlinkPutObjectResult(PutObjectResponse response) {
        return new FlinkPutObjectResult(response.eTag(), response.versionId());
    }

    // ========== CompleteMultipartUploadResponse Conversions ==========

    /**
     * Converts an AWS SDK v2 CompleteMultipartUploadResponse to a Flink
     * CompleteMultipartUploadResult.
     *
     * @param response the AWS SDK v2 CompleteMultipartUploadResponse
     * @return the Flink CompleteMultipartUploadResult
     */
    public static FlinkCompleteMultipartUploadResult toFlinkCompleteMultipartUploadResult(
            CompleteMultipartUploadResponse response) {
        return FlinkCompleteMultipartUploadResult.builder()
                .bucket(response.bucket())
                .key(response.key())
                .eTag(response.eTag())
                .location(response.location())
                .versionId(response.versionId())
                .build();
    }

    // ========== HeadObjectResponse Conversions ==========

    /**
     * Converts an AWS SDK v2 HeadObjectResponse to a Flink ObjectMetadata.
     *
     * @param response the AWS SDK v2 HeadObjectResponse
     * @return the Flink ObjectMetadata
     */
    public static FlinkObjectMetadata toFlinkObjectMetadata(HeadObjectResponse response) {
        return FlinkObjectMetadata.builder()
                .contentLength(response.contentLength())
                .contentType(response.contentType())
                .eTag(response.eTag())
                .lastModified(
                        response.lastModified() != null ? Date.from(response.lastModified()) : null)
                .userMetadata(response.metadata())
                .build();
    }

    // ========== GetObjectResponse Conversions ==========

    /**
     * Converts an AWS SDK v2 GetObjectResponse to a Flink ObjectMetadata.
     *
     * @param response the AWS SDK v2 GetObjectResponse
     * @return the Flink ObjectMetadata
     */
    public static FlinkObjectMetadata toFlinkObjectMetadata(GetObjectResponse response) {
        return FlinkObjectMetadata.builder()
                .contentLength(response.contentLength())
                .contentType(response.contentType())
                .eTag(response.eTag())
                .lastModified(
                        response.lastModified() != null ? Date.from(response.lastModified()) : null)
                .userMetadata(response.metadata())
                .build();
    }
}
