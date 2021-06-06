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

package org.apache.flink.streaming.connectors.dynamodb.retry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

/** A collection of utility functions to simplify work with DynamoDB service exceptions. */
public class DynamoDbExceptionUtils {

    public static boolean isServiceException(Exception e) {
        return e instanceof AwsServiceException;
    }

    public static boolean isResourceNotFoundException(Exception e) {
        return e instanceof ResourceNotFoundException;
    }

    public static boolean isConditionalCheckFailedException(Exception e) {
        return e instanceof ConditionalCheckFailedException;
    }

    public static boolean isThrottlingException(Exception e) {
        return isServiceException(e) && ((AwsServiceException) e).isThrottlingException();
    }

    public static boolean isNotRetryableException(Exception e) {
        return isResourceNotFoundException(e) || isConditionalCheckFailedException(e);
    }
}
