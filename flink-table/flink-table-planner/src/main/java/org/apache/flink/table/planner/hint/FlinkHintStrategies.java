/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.hint;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.plan.rules.logical.WrapJsonAggFunctionArgumentsRule;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.apache.calcite.rel.hint.HintOptionChecker;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.util.Litmus;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

/** A collection of Flink style {@link HintStrategy}s. */
public abstract class FlinkHintStrategies {

    /**
     * Customize the {@link HintStrategyTable} which contains hint strategies supported by Flink.
     */
    public static HintStrategyTable createHintStrategyTable() {
        return HintStrategyTable.builder()
                // Configure to always throw when we encounter any hint errors
                // (either the non-registered hint or the hint format).
                .errorHandler(Litmus.THROW)
                .hintStrategy(
                        FlinkHints.HINT_NAME_OPTIONS,
                        HintStrategy.builder(HintPredicates.TABLE_SCAN)
                                .optionChecker(OPTIONS_KV_OPTION_CHECKER)
                                .build())
                .hintStrategy(
                        FlinkHints.HINT_NAME_JSON_AGGREGATE_WRAPPED,
                        HintStrategy.builder(HintPredicates.AGGREGATE)
                                .excludedRules(WrapJsonAggFunctionArgumentsRule.INSTANCE)
                                .build())
                // internal join hint used for alias
                .hintStrategy(
                        FlinkHints.HINT_ALIAS,
                        // currently, only correlate&join hints care about query block alias
                        HintStrategy.builder(
                                        HintPredicates.or(
                                                HintPredicates.CORRELATE, HintPredicates.JOIN))
                                .optionChecker(fixedSizeListOptionChecker(1))
                                .build())
                // TODO semi/anti join with CORRELATE is not supported
                .hintStrategy(
                        JoinStrategy.BROADCAST.getJoinHintName(),
                        HintStrategy.builder(HintPredicates.JOIN)
                                .optionChecker(NON_EMPTY_LIST_OPTION_CHECKER)
                                .build())
                .hintStrategy(
                        JoinStrategy.SHUFFLE_HASH.getJoinHintName(),
                        HintStrategy.builder(HintPredicates.JOIN)
                                .optionChecker(NON_EMPTY_LIST_OPTION_CHECKER)
                                .build())
                .hintStrategy(
                        JoinStrategy.SHUFFLE_MERGE.getJoinHintName(),
                        HintStrategy.builder(HintPredicates.JOIN)
                                .optionChecker(NON_EMPTY_LIST_OPTION_CHECKER)
                                .build())
                .hintStrategy(
                        JoinStrategy.NEST_LOOP.getJoinHintName(),
                        HintStrategy.builder(HintPredicates.JOIN)
                                .optionChecker(NON_EMPTY_LIST_OPTION_CHECKER)
                                .build())
                .hintStrategy(
                        JoinStrategy.LOOKUP.getJoinHintName(),
                        HintStrategy.builder(
                                        HintPredicates.or(
                                                HintPredicates.CORRELATE, HintPredicates.JOIN))
                                .optionChecker(LOOKUP_NON_EMPTY_KV_OPTION_CHECKER)
                                .build())
                .build();
    }

    private static HintOptionChecker fixedSizeListOptionChecker(int size) {
        return (hint, errorHandler) ->
                errorHandler.check(
                        hint.listOptions.size() == size,
                        "Invalid hint: {}, expecting {} table or view {} "
                                + "specified in hint {}.",
                        FlinkHints.stringifyHints(Collections.singletonList(hint)),
                        size,
                        size > 1 ? "names" : "name",
                        hint.hintName);
    }

    // ~ hint option checker ------------------------------------------------------------

    private static final HintOptionChecker NON_EMPTY_LIST_OPTION_CHECKER =
            (hint, errorHandler) ->
                    errorHandler.check(
                            hint.listOptions.size() > 0,
                            "Invalid hint: {}, expecting at least "
                                    + "one table or view specified in hint {}.",
                            FlinkHints.stringifyHints(Collections.singletonList(hint)),
                            hint.hintName);

    private static final HintOptionChecker OPTIONS_KV_OPTION_CHECKER =
            (hint, errorHandler) -> {
                errorHandler.check(
                        hint.kvOptions.size() > 0,
                        "Hint [{}] only support non empty key value options",
                        hint.hintName);

                Configuration conf = Configuration.fromMap(hint.kvOptions);
                Optional<String> errMsgOptional = FactoryUtil.checkWatermarkOptions(conf);
                errorHandler.check(
                        !errMsgOptional.isPresent(), errMsgOptional.orElse("No errors."));
                return true;
            };

    private static final HintOptionChecker LOOKUP_NON_EMPTY_KV_OPTION_CHECKER =
            (lookupHint, litmus) -> {
                litmus.check(
                        lookupHint.listOptions.size() == 0,
                        "Invalid list options in LOOKUP hint, only support key-value options.");

                Configuration conf = Configuration.fromMap(lookupHint.kvOptions);
                ImmutableSet<ConfigOption> requiredKeys =
                        LookupJoinHintOptions.getRequiredOptions();
                litmus.check(
                        requiredKeys.stream().allMatch(conf::contains),
                        "Invalid LOOKUP hint: incomplete required option(s): {}",
                        requiredKeys);

                ImmutableSet<ConfigOption> supportedKeys =
                        LookupJoinHintOptions.getSupportedOptions();
                litmus.check(
                        lookupHint.kvOptions.size() <= supportedKeys.size(),
                        "Too many LOOKUP hint options {} beyond max number of supported options {}",
                        lookupHint.kvOptions.size(),
                        supportedKeys.size());

                try {
                    // try to validate all hint options by parsing them
                    supportedKeys.forEach(conf::get);
                } catch (IllegalArgumentException e) {
                    litmus.fail("Invalid LOOKUP hint options: {}", e.getMessage());
                }

                // option value check
                // async options are all optional
                Boolean async = conf.get(LookupJoinHintOptions.ASYNC_LOOKUP);
                if (Boolean.TRUE.equals(async)) {
                    Integer capacity = conf.get(LookupJoinHintOptions.ASYNC_CAPACITY);
                    litmus.check(
                            null == capacity || capacity > 0,
                            "Invalid LOOKUP hint option: {} value should be positive integer but was {}",
                            LookupJoinHintOptions.ASYNC_CAPACITY.key(),
                            capacity);
                }

                // retry options can be both null or all not null
                String retryPredicate = conf.get(LookupJoinHintOptions.RETRY_PREDICATE);
                LookupJoinHintOptions.RetryStrategy retryStrategy =
                        conf.get(LookupJoinHintOptions.RETRY_STRATEGY);
                Duration fixedDelay = conf.get(LookupJoinHintOptions.FIXED_DELAY);
                Integer maxAttempts = conf.get(LookupJoinHintOptions.MAX_ATTEMPTS);
                litmus.check(
                        (null == retryPredicate
                                        && null == retryStrategy
                                        && null == fixedDelay
                                        && null == fixedDelay
                                        && null == maxAttempts)
                                || (null != retryPredicate
                                        && null != retryStrategy
                                        && null != fixedDelay
                                        && null != fixedDelay
                                        && null != maxAttempts),
                        "Invalid LOOKUP hint: retry options can be both null or all not null");

                // if with retry options, all values should be valid
                if (null != retryPredicate) {
                    litmus.check(
                            LookupJoinHintOptions.LOOKUP_MISS_PREDICATE.equalsIgnoreCase(
                                    retryPredicate),
                            "Invalid LOOKUP hint option: unsupported {} '{}', only '{}' is supported currently",
                            LookupJoinHintOptions.RETRY_PREDICATE.key(),
                            retryPredicate,
                            LookupJoinHintOptions.LOOKUP_MISS_PREDICATE);
                    litmus.check(
                            maxAttempts > 0,
                            "Invalid LOOKUP hint option: {} value should be positive integer but was {}",
                            LookupJoinHintOptions.MAX_ATTEMPTS.key(),
                            maxAttempts);
                }
                return true;
            };
}
