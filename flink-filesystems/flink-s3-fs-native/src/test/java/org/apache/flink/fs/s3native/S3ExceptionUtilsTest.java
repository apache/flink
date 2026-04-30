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

package org.apache.flink.fs.s3native;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link S3ExceptionUtils}. */
class S3ExceptionUtilsTest {

    static Stream<Arguments> toIoExceptionCases() {
        return Stream.of(
                Arguments.of(
                        s3Exception(
                                404,
                                AwsErrorDetails.builder().errorMessage("key not found").build()),
                        "delete object",
                        "delete object (HTTP 404: key not found)"),
                Arguments.of(
                        s3Exception(500, "internal error"),
                        "put object",
                        "put object (HTTP 500: internal error"));
    }

    @ParameterizedTest
    @MethodSource("toIoExceptionCases")
    void toIOException_contextAndStatusCode_formatsMessageAndPreservesCause(
            S3Exception cause, String context, String expectedMessage) {
        IOException result = S3ExceptionUtils.toIOException(context, cause);

        assertThat(result.getMessage()).contains(expectedMessage);
        assertThat(result.getCause()).isSameAs(cause);
    }

    @Test
    void errorMessage_detailsHasMessage_returnsDetailsMessage() {
        S3Exception e =
                s3Exception(
                        404,
                        AwsErrorDetails.builder()
                                .errorMessage("The specified key does not exist.")
                                .build());

        assertThat(S3ExceptionUtils.errorMessage(e)).isEqualTo("The specified key does not exist.");
    }

    static Stream<Arguments> errorMessageFallbackCases() {
        return Stream.of(
                Arguments.of(s3Exception(500, "connection timeout"), "connection timeout"),
                Arguments.of(
                        s3ExceptionWithMessageAndDetails(
                                500,
                                "fallback message",
                                AwsErrorDetails.builder().errorCode("InternalError").build()),
                        "fallback message"));
    }

    @ParameterizedTest
    @MethodSource("errorMessageFallbackCases")
    void errorMessage_noDetailsMessage_fallsBackToExceptionMessage(
            S3Exception e, String expectedMessage) {
        assertThat(S3ExceptionUtils.errorMessage(e)).contains(expectedMessage);
    }

    @Test
    void errorMessage_noMessageAvailable_returnsUnknownS3Error() {
        S3Exception e = s3ExceptionStatusOnly(500);

        assertThat(S3ExceptionUtils.errorMessage(e)).isEqualTo("Unknown S3 error");
    }

    @Test
    void errorCode_detailsHasCode_returnsErrorCode() {
        S3Exception e = s3Exception(404, AwsErrorDetails.builder().errorCode("NoSuchKey").build());

        assertThat(S3ExceptionUtils.errorCode(e)).isEqualTo("NoSuchKey");
    }

    static Stream<Arguments> errorCodeUnknownCases() {
        return Stream.of(
                Arguments.of(s3Exception(500, "some error")),
                Arguments.of(
                        s3Exception(
                                500,
                                AwsErrorDetails.builder()
                                        .errorMessage("Something went wrong")
                                        .build())));
    }

    @ParameterizedTest
    @MethodSource("errorCodeUnknownCases")
    void errorCode_noCodeAvailable_returnsUnknown(S3Exception e) {
        assertThat(S3ExceptionUtils.errorCode(e)).isEqualTo("Unknown");
    }

    private static S3Exception s3Exception(int statusCode, AwsErrorDetails details) {
        S3Exception.Builder b = S3Exception.builder();
        b.statusCode(statusCode);
        b.awsErrorDetails(details);
        return (S3Exception) b.build();
    }

    private static S3Exception s3Exception(int statusCode, String message) {
        S3Exception.Builder b = S3Exception.builder();
        b.statusCode(statusCode);
        b.message(message);
        return (S3Exception) b.build();
    }

    private static S3Exception s3ExceptionWithMessageAndDetails(
            int statusCode, String message, AwsErrorDetails details) {
        S3Exception.Builder b = S3Exception.builder();
        b.statusCode(statusCode);
        b.message(message);
        b.awsErrorDetails(details);
        return (S3Exception) b.build();
    }

    private static S3Exception s3ExceptionStatusOnly(int statusCode) {
        S3Exception.Builder b = S3Exception.builder();
        b.statusCode(statusCode);
        return (S3Exception) b.build();
    }
}
