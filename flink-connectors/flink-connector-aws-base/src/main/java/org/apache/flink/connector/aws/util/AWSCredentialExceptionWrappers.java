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

package org.apache.flink.connector.aws.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ThrowableWrapper;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sts.model.StsException;

/** Class containing Exception Wrappers for AWS credenetial failures. */
@Internal
public class AWSCredentialExceptionWrappers {
    public static final ThrowableWrapper INVALID_CREDENTIALS_STRATEGY =
            ThrowableWrapper.withRootCauseOfType(
                    StsException.class,
                    err ->
                            new AWSAuthenticationException(
                                    "Encountered non-recoverable exception relating to the provided credentials.",
                                    err));

    public static final ThrowableWrapper SDK_CLIENT_MISCONFIGURED_STRATEGY =
            ThrowableWrapper.withRootCauseOfType(
                    SdkClientException.class,
                    err ->
                            new AWSAuthenticationException(
                                    "Encountered non-recoverable exception relating to mis-configured client",
                                    err));

    public static final ThrowableWrapper MISSING_ACCESS_KEY_ID_STRATEGY =
            new ThrowableWrapper(
                    err ->
                            ExceptionUtils.findThrowableWithMessage(
                                                    err, "Access key ID cannot be blank.")
                                            .isPresent()
                                    || ExceptionUtils.findThrowableWithMessage(
                                                    err,
                                                    "Either the environment variable AWS_WEB_IDENTITY_TOKEN_FILE or the javaproperty aws.webIdentityTokenFile must be set.")
                                            .isPresent(),
                    err ->
                            new AWSAuthenticationException(
                                    "Encountered non-recoverable exception relating to missing credentials",
                                    err));
}
