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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.hint.LookupJoinHintOptions;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.hint.RelHint;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.table.planner.hint.LookupJoinHintOptions.ASYNC_CAPACITY;
import static org.apache.flink.table.planner.hint.LookupJoinHintOptions.ASYNC_LOOKUP;
import static org.apache.flink.table.planner.hint.LookupJoinHintOptions.ASYNC_OUTPUT_MODE;
import static org.apache.flink.table.planner.hint.LookupJoinHintOptions.ASYNC_TIMEOUT;
import static org.apache.flink.table.planner.hint.LookupJoinHintOptions.FIXED_DELAY;
import static org.apache.flink.table.planner.hint.LookupJoinHintOptions.MAX_ATTEMPTS;
import static org.apache.flink.table.planner.hint.LookupJoinHintOptions.RETRY_PREDICATE;
import static org.apache.flink.table.planner.hint.LookupJoinHintOptions.RETRY_STRATEGY;
import static org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy.NO_RETRY_STRATEGY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * LookupJoinHintSpec describes the user specified hint options for lookup join.
 *
 * <p>This class corresponds to {@link
 * org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLookupJoin} rel node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LookupJoinHintSpec {
    public static final String FIELD_NAME_ASYNC = "async";
    public static final String FIELD_NAME_ASYNC_OUTPUT_MODE = "output-mode";
    public static final String FIELD_NAME_ASYNC_CAPACITY = "capacity";
    public static final String FIELD_NAME_ASYNC_TIMEOUT = "timeout";
    public static final String FIELD_NAME_RETRY_PREDICATE = "retry-predicate";
    public static final String FIELD_NAME_RETRY_STRATEGY = "retry-strategy";
    public static final String FIELD_NAME_RETRY_FIXED_DELAY = "fixed-delay";
    public static final String FIELD_NAME_RETRY_MAX_ATTEMPTS = "max-attempts";

    @JsonProperty(FIELD_NAME_ASYNC)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable Boolean async;

    @JsonProperty(FIELD_NAME_ASYNC_OUTPUT_MODE)
    private final ExecutionConfigOptions.AsyncOutputMode asyncOutputMode;

    @JsonProperty(FIELD_NAME_ASYNC_CAPACITY)
    private final Integer asyncCapacity;

    @JsonProperty(FIELD_NAME_ASYNC_TIMEOUT)
    private final Long asyncTimeout;

    @JsonProperty(FIELD_NAME_RETRY_PREDICATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable String retryPredicate;

    @JsonProperty(FIELD_NAME_RETRY_STRATEGY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable LookupJoinHintOptions.RetryStrategy retryStrategy;

    @JsonProperty(FIELD_NAME_RETRY_FIXED_DELAY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable Long retryFixedDelay;

    @JsonProperty(FIELD_NAME_RETRY_MAX_ATTEMPTS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable Integer retryMaxAttempts;

    @JsonCreator
    public LookupJoinHintSpec(
            @JsonProperty(FIELD_NAME_ASYNC) @Nullable Boolean async,
            @JsonProperty(FIELD_NAME_ASYNC_OUTPUT_MODE) @Nullable
                    ExecutionConfigOptions.AsyncOutputMode asyncOutputMode,
            @JsonProperty(FIELD_NAME_ASYNC_CAPACITY) Integer asyncCapacity,
            @JsonProperty(FIELD_NAME_ASYNC_TIMEOUT) Long asyncTimeout,
            @JsonProperty(FIELD_NAME_RETRY_PREDICATE) String retryPredicate,
            @JsonProperty(FIELD_NAME_RETRY_STRATEGY) @Nullable
                    LookupJoinHintOptions.RetryStrategy retryStrategy,
            @JsonProperty(FIELD_NAME_RETRY_FIXED_DELAY) @Nullable Long retryFixedDelay,
            @JsonProperty(FIELD_NAME_RETRY_MAX_ATTEMPTS) @Nullable Integer retryMaxAttempts) {
        this.async = async;
        this.asyncOutputMode = checkNotNull(asyncOutputMode);
        this.asyncCapacity = checkNotNull(asyncCapacity);
        this.asyncTimeout = checkNotNull(asyncTimeout);
        this.retryPredicate = retryPredicate;
        this.retryStrategy = retryStrategy;
        this.retryFixedDelay = retryFixedDelay;
        this.retryMaxAttempts = retryMaxAttempts;
    }

    @JsonIgnore
    @Nullable
    public Boolean getAsync() {
        return async;
    }

    @JsonIgnore
    public Boolean isAsync() {
        return Boolean.TRUE.equals(async);
    }

    @JsonIgnore
    public ExecutionConfigOptions.AsyncOutputMode getAsyncOutputMode() {
        return asyncOutputMode;
    }

    @JsonIgnore
    public Integer getAsyncCapacity() {
        return asyncCapacity;
    }

    @JsonIgnore
    public Long getAsyncTimeout() {
        return asyncTimeout;
    }

    @JsonIgnore
    public String getRetryPredicate() {
        return retryPredicate;
    }

    @JsonIgnore
    @Nullable
    public LookupJoinHintOptions.RetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    @JsonIgnore
    @Nullable
    public Long getRetryFixedDelay() {
        return retryFixedDelay;
    }

    @JsonIgnore
    @Nullable
    public Integer getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LookupJoinHintSpec that = (LookupJoinHintSpec) o;
        return Objects.equals(async, that.async)
                && asyncOutputMode == that.asyncOutputMode
                && Objects.equals(asyncCapacity, that.asyncCapacity)
                && Objects.equals(asyncTimeout, that.asyncTimeout)
                && Objects.equals(retryPredicate, that.retryPredicate)
                && retryStrategy == that.retryStrategy
                && Objects.equals(retryFixedDelay, that.retryFixedDelay)
                && Objects.equals(retryMaxAttempts, that.retryMaxAttempts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                async,
                asyncOutputMode,
                asyncCapacity,
                asyncTimeout,
                retryPredicate,
                retryStrategy,
                retryFixedDelay,
                retryMaxAttempts);
    }

    /**
     * Convert given joinHint to {@link LookupJoinHintSpec}.
     *
     * @param lookupJoinHint
     */
    @JsonIgnore
    public static LookupJoinHintSpec fromJoinHint(RelHint lookupJoinHint) {
        Configuration conf = Configuration.fromMap(lookupJoinHint.kvOptions);
        Duration fixedDelay = conf.get(FIXED_DELAY);
        return new LookupJoinHintSpec(
                conf.get(ASYNC_LOOKUP),
                conf.get(ASYNC_OUTPUT_MODE),
                conf.get(ASYNC_CAPACITY),
                conf.get(ASYNC_TIMEOUT).toMillis(),
                conf.get(RETRY_PREDICATE),
                conf.get(RETRY_STRATEGY),
                fixedDelay != null ? fixedDelay.toMillis() : null,
                conf.get(MAX_ATTEMPTS));
    }

    /**
     * Convert this {@link LookupJoinHintSpec} to {@link ResultRetryStrategy} in a best effort
     * manner. If invalid {@link LookupJoinHintOptions#RETRY_PREDICATE} or {@link
     * LookupJoinHintOptions#RETRY_STRATEGY} is given, then {@link
     * ResultRetryStrategy#NO_RETRY_STRATEGY} will return.
     */
    @JsonIgnore
    public ResultRetryStrategy toRetryStrategy() {
        if (null == retryPredicate
                || !LookupJoinHintOptions.LOOKUP_MISS_PREDICATE.equalsIgnoreCase(retryPredicate)
                || retryStrategy != LookupJoinHintOptions.RetryStrategy.FIXED_DELAY) {
            return NO_RETRY_STRATEGY;
        }
        // retry option values have been validated by hint checker
        return ResultRetryStrategy.fixedDelayRetry(
                this.retryMaxAttempts,
                this.retryFixedDelay,
                RetryPredicates.EMPTY_RESULT_PREDICATE);
    }
}
