/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.copied.common.protocol;

import org.apache.kafka.copied.common.errors.ApiException;
import org.apache.kafka.copied.common.errors.ConsumerCoordinatorNotAvailableException;
import org.apache.kafka.copied.common.errors.CorruptRecordException;
import org.apache.kafka.copied.common.errors.IllegalGenerationException;
import org.apache.kafka.copied.common.errors.InvalidRequiredAcksException;
import org.apache.kafka.copied.common.errors.InvalidTopicException;
import org.apache.kafka.copied.common.errors.LeaderNotAvailableException;
import org.apache.kafka.copied.common.errors.NetworkException;
import org.apache.kafka.copied.common.errors.NotCoordinatorForConsumerException;
import org.apache.kafka.copied.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.copied.common.errors.NotEnoughReplicasException;
import org.apache.kafka.copied.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.copied.common.errors.OffsetLoadInProgressException;
import org.apache.kafka.copied.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.copied.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.copied.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.copied.common.errors.RecordTooLargeException;
import org.apache.kafka.copied.common.errors.TimeoutException;
import org.apache.kafka.copied.common.errors.UnknownConsumerIdException;
import org.apache.kafka.copied.common.errors.UnknownServerException;
import org.apache.kafka.copied.common.errors.UnknownTopicOrPartitionException;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 * 
 * Do not add exceptions that occur only on the client or only on the server here.
 */
public enum Errors {
    UNKNOWN(-1, new UnknownServerException("The server experienced an unexpected error when processing the request")),
    NONE(0, null),
    OFFSET_OUT_OF_RANGE(1,
            new OffsetOutOfRangeException("The requested offset is not within the range of offsets maintained by the server.")),
    CORRUPT_MESSAGE(2,
            new CorruptRecordException("The message contents does not match the message CRC or the message is otherwise corrupt.")),
    UNKNOWN_TOPIC_OR_PARTITION(3,
            new UnknownTopicOrPartitionException("This server does not host this topic-partition.")),
    // TODO: errorCode 4 for InvalidFetchSize
    LEADER_NOT_AVAILABLE(5,
            new LeaderNotAvailableException("There is no leader for this topic-partition as we are in the middle of a leadership election.")),
    NOT_LEADER_FOR_PARTITION(6,
            new NotLeaderForPartitionException("This server is not the leader for that topic-partition.")),
    REQUEST_TIMED_OUT(7,
            new TimeoutException("The request timed out.")),
    // TODO: errorCode 8 for BrokerNotAvailable
    REPLICA_NOT_AVAILABLE(9,
            new ApiException("The replica is not available for the requested topic-partition")),
    MESSAGE_TOO_LARGE(10,
            new RecordTooLargeException("The request included a message larger than the max message size the server will accept.")),
    OFFSET_METADATA_TOO_LARGE(12,
            new OffsetMetadataTooLarge("The metadata field of the offset request was too large.")),
    NETWORK_EXCEPTION(13,
            new NetworkException("The server disconnected before a response was received.")),
    OFFSET_LOAD_IN_PROGRESS(14,
            new OffsetLoadInProgressException("The coordinator is loading offsets and can't process requests.")),
    CONSUMER_COORDINATOR_NOT_AVAILABLE(15,
            new ConsumerCoordinatorNotAvailableException("The coordinator is not available.")),
    NOT_COORDINATOR_FOR_CONSUMER(16,
            new NotCoordinatorForConsumerException("This is not the correct coordinator for this consumer.")),
    INVALID_TOPIC_EXCEPTION(17,
            new InvalidTopicException("The request attempted to perform an operation on an invalid topic.")),
    RECORD_LIST_TOO_LARGE(18,
            new RecordBatchTooLargeException("The request included message batch larger than the configured segment size on the server.")),
    NOT_ENOUGH_REPLICAS(19,
            new NotEnoughReplicasException("Messages are rejected since there are fewer in-sync replicas than required.")),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(20,
            new NotEnoughReplicasAfterAppendException("Messages are written to the log, but to fewer in-sync replicas than required.")),
    INVALID_REQUIRED_ACKS(21,
            new InvalidRequiredAcksException("Produce request specified an invalid value for required acks.")),
    ILLEGAL_GENERATION(22,
            new IllegalGenerationException("Specified consumer generation id is not valid.")),
    INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY(23,
            new ApiException("The request partition assignment strategy does not match that of the group.")),
    UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY(24,
            new ApiException("The request partition assignment strategy is unknown to the broker.")),
    UNKNOWN_CONSUMER_ID(25,
            new UnknownConsumerIdException("The coordinator is not aware of this consumer.")),
    INVALID_SESSION_TIMEOUT(26,
            new ApiException("The session timeout is not within an acceptable range.")),
    COMMITTING_PARTITIONS_NOT_ASSIGNED(27,
            new ApiException("Some of the committing partitions are not assigned the committer")),
    INVALID_COMMIT_OFFSET_SIZE(28,
            new ApiException("The committing offset data size is not valid"));

    private static Map<Class<?>, Errors> classToError = new HashMap<Class<?>, Errors>();
    private static Map<Short, Errors> codeToError = new HashMap<Short, Errors>();

    static {
        for (Errors error : Errors.values()) {
            codeToError.put(error.code(), error);
            if (error.exception != null)
                classToError.put(error.exception.getClass(), error);
        }
    }

    private final short code;
    private final ApiException exception;

    private Errors(int code, ApiException exception) {
        this.code = (short) code;
        this.exception = exception;
    }

    /**
     * An instance of the exception
     */
    public ApiException exception() {
        return this.exception;
    }

    /**
     * The error code for the exception
     */
    public short code() {
        return this.code;
    }

    /**
     * Throw the exception corresponding to this error if there is one
     */
    public void maybeThrow() {
        if (exception != null) {
            throw this.exception;
        }
    }

    /**
     * Throw the exception if there is one
     */
    public static Errors forCode(short code) {
        Errors error = codeToError.get(code);
        return error == null ? UNKNOWN : error;
    }

    /**
     * Return the error instance associated with this exception (or UKNOWN if there is none)
     */
    public static Errors forException(Throwable t) {
        Errors error = classToError.get(t.getClass());
        return error == null ? UNKNOWN : error;
    }
}
