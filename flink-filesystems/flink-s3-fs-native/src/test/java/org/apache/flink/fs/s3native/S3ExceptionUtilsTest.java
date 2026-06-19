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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
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
                        "put object (HTTP 500: internal error)"));
    }

    @ParameterizedTest
    @MethodSource("toIoExceptionCases")
    void toIOException_contextAndStatusCode_formatsMessageAndPreservesCause(
            S3Exception cause, String context, String expectedMessage) {
        IOException result = S3ExceptionUtils.toIOException(context, cause);

        assertThat(result.getMessage()).contains(expectedMessage);
        assertThat(result.getCause()).isSameAs(cause);
    }

    static Stream<Arguments> errorMessageExactCases() {
        return Stream.of(
                Arguments.of(
                        s3Exception(
                                404,
                                AwsErrorDetails.builder()
                                        .errorMessage("The specified key does not exist.")
                                        .build()),
                        "The specified key does not exist."),
                Arguments.of(s3ExceptionStatusOnly(500), "Unknown S3 error"));
    }

    @ParameterizedTest
    @MethodSource("errorMessageExactCases")
    void errorMessage_exactReturn_matchesExpected(S3Exception e, String expected) {
        assertThat(S3ExceptionUtils.errorMessage(e)).isEqualTo(expected);
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
    void errorMessage_fallbackToExceptionMessage_containsExpected(
            S3Exception e, String expectedMessage) {
        assertThat(S3ExceptionUtils.errorMessage(e)).contains(expectedMessage);
    }

    static Stream<Arguments> errorCodeAllCases() {
        return Stream.of(
                Arguments.of(
                        s3Exception(404, AwsErrorDetails.builder().errorCode("NoSuchKey").build()),
                        "NoSuchKey"),
                Arguments.of(s3Exception(500, "some error"), "Unknown"),
                Arguments.of(
                        s3Exception(
                                500,
                                AwsErrorDetails.builder()
                                        .errorMessage("Something went wrong")
                                        .build()),
                        "Unknown"));
    }

    @ParameterizedTest
    @MethodSource("errorCodeAllCases")
    void errorCode_allCases_returnsExpected(S3Exception e, String expected) {
        assertThat(S3ExceptionUtils.errorCode(e)).isEqualTo(expected);
    }

    private static AwsServiceException s3Exception(int statusCode, AwsErrorDetails details) {
        return S3Exception.builder().statusCode(statusCode).awsErrorDetails(details).build();
    }

    private static AwsServiceException s3Exception(int statusCode, String message) {
        return S3Exception.builder().statusCode(statusCode).message(message).build();
    }

    private static AwsServiceException s3ExceptionWithMessageAndDetails(
            int statusCode, String message, AwsErrorDetails details) {
        return S3Exception.builder()
                .statusCode(statusCode)
                .message(message)
                .awsErrorDetails(details)
                .build();
    }

    private static AwsServiceException s3ExceptionStatusOnly(int statusCode) {
        return S3Exception.builder().statusCode(statusCode).build();
    }
}
