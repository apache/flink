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

package org.apache.flink.runtime.failure;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricher.Context;
import org.apache.flink.core.failure.FailureEnricherFactory;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utils class for loading and running pluggable failure enrichers. */
public class FailureEnricherUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FailureEnricherUtils.class);

    public static final CompletableFuture<Map<String, String>> EMPTY_FAILURE_LABELS =
            CompletableFuture.completedFuture(Collections.emptyMap());

    // regex pattern to split the defined failure enrichers
    private static final Pattern enricherListPattern = Pattern.compile("\\s*,\\s*");
    static final String MERGE_EXCEPTION_MSG =
            "Trying to merge a label with a duplicate key %s. This is a bug that should be reported,"
                    + " because Flink shouldn't allow registering enrichers with the same output.";

    /**
     * Returns a set of validated FailureEnrichers for a given configuration.
     *
     * @param configuration the configuration for the job
     * @return a collection of validated FailureEnrichers
     */
    public static Collection<FailureEnricher> getFailureEnrichers(
            final Configuration configuration) {
        final PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configuration);
        return getFailureEnrichers(configuration, pluginManager);
    }

    @VisibleForTesting
    static Collection<FailureEnricher> getFailureEnrichers(
            final Configuration configuration, final PluginManager pluginManager) {
        final Set<String> enrichersToLoad = getIncludedFailureEnrichers(configuration);
        //  When empty, NO enrichers will be started.
        if (enrichersToLoad.isEmpty()) {
            return Collections.emptySet();
        }
        final Iterator<FailureEnricherFactory> factoryIterator =
                pluginManager.load(FailureEnricherFactory.class);
        final Set<FailureEnricher> failureEnrichers = new HashSet<>();
        while (factoryIterator.hasNext()) {
            final FailureEnricherFactory failureEnricherFactory = factoryIterator.next();
            final FailureEnricher failureEnricher =
                    failureEnricherFactory.createFailureEnricher(configuration);
            if (enrichersToLoad.remove(failureEnricher.getClass().getName())) {
                failureEnrichers.add(failureEnricher);
                LOG.info(
                        "Found failure enricher {} at {}.",
                        failureEnricherFactory.getClass().getName(),
                        failureEnricher
                                .getClass()
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .getPath());
            } else {
                LOG.debug(
                        "Excluding failure enricher {}, not configured in enricher list ({}).",
                        failureEnricherFactory.getClass().getName(),
                        enrichersToLoad);
            }
        }

        if (!enrichersToLoad.isEmpty()) {
            LOG.error(
                    "The following failure enrichers were configured but not found on the classpath: {}.",
                    enrichersToLoad);
        }

        return filterInvalidEnrichers(failureEnrichers);
    }

    /**
     * Returns a set of failure enricher names included in the given configuration.
     *
     * @param configuration the configuration to get the failure enricher names from
     * @return failure enricher names
     */
    @VisibleForTesting
    static Set<String> getIncludedFailureEnrichers(final Configuration configuration) {
        final String includedEnrichersString =
                configuration.getString(JobManagerOptions.FAILURE_ENRICHERS_LIST, "");
        return enricherListPattern
                .splitAsStream(includedEnrichersString)
                .filter(r -> !r.isEmpty())
                .collect(Collectors.toSet());
    }

    /**
     * Filters out invalid {@link FailureEnricher} objects that have duplicate output keys.
     *
     * @param failureEnrichers a set of {@link FailureEnricher} objects to filter
     * @return a filtered collection without any duplicate output keys
     */
    @VisibleForTesting
    static Collection<FailureEnricher> filterInvalidEnrichers(
            final Set<FailureEnricher> failureEnrichers) {
        final Map<String, Set<Class<?>>> enrichersByKey = new HashMap<>();
        failureEnrichers.forEach(
                enricher ->
                        enricher.getOutputKeys()
                                .forEach(
                                        enricherKey ->
                                                enrichersByKey
                                                        .computeIfAbsent(
                                                                enricherKey,
                                                                ignored -> new HashSet<>())
                                                        .add(enricher.getClass())));
        final Set<Class<?>> invalidEnrichers =
                enrichersByKey.entrySet().stream()
                        .filter(entry -> entry.getValue().size() > 1)
                        .flatMap(
                                entry -> {
                                    LOG.warn(
                                            "Following enrichers have have registered duplicate output key [%s] and will be ignored: {}.",
                                            entry.getValue().stream()
                                                    .map(Class::getName)
                                                    .collect(Collectors.joining(", ")));
                                    return entry.getValue().stream();
                                })
                        .collect(Collectors.toSet());
        return failureEnrichers.stream()
                .filter(enricher -> !invalidEnrichers.contains(enricher.getClass()))
                .collect(Collectors.toList());
    }

    /**
     * Enriches a Throwable by returning the merged label output of a Set of FailureEnrichers.
     *
     * @param cause the Throwable to label
     * @param context the context of the Throwable
     * @param mainThreadExecutor the executor to complete the enricher labeling on
     * @param failureEnrichers a collection of FailureEnrichers to enrich the context with
     * @return a CompletableFuture that will complete with a map of labels
     */
    public static CompletableFuture<Map<String, String>> labelFailure(
            final Throwable cause,
            final Context context,
            final Executor mainThreadExecutor,
            final Collection<FailureEnricher> failureEnrichers) {
        // list of CompletableFutures to enrich failure with labels from each enricher
        final Collection<CompletableFuture<Map<String, String>>> enrichFutures = new ArrayList<>();

        for (final FailureEnricher enricher : failureEnrichers) {
            enrichFutures.add(
                    enricher.processFailure(cause, context)
                            .thenApply(
                                    enricherLabels -> {
                                        final Map<String, String> validLabels = new HashMap<>();
                                        enricherLabels.forEach(
                                                (k, v) -> {
                                                    if (!enricher.getOutputKeys().contains(k)) {
                                                        LOG.warn(
                                                                "Ignoring label with key {} from enricher {}"
                                                                        + " violating contract, keys allowed {}.",
                                                                k,
                                                                enricher.getClass(),
                                                                enricher.getOutputKeys());
                                                    } else {
                                                        validLabels.put(k, v);
                                                    }
                                                });
                                        return validLabels;
                                    })
                            .exceptionally(
                                    t -> {
                                        LOG.warn(
                                                "Enricher {} threw an exception.",
                                                enricher.getClass(),
                                                t);
                                        return Collections.emptyMap();
                                    }));
        }
        // combine all CompletableFutures into a single CompletableFuture containing a Map of labels
        return FutureUtils.combineAll(enrichFutures)
                .thenApplyAsync(
                        labelsToMerge -> {
                            final Map<String, String> mergedLabels = new HashMap<>();
                            for (Map<String, String> labels : labelsToMerge) {
                                labels.forEach(
                                        (k, v) ->
                                                // merge label with existing, throwing an exception
                                                // if there is a key conflict
                                                mergedLabels.merge(
                                                        k,
                                                        v,
                                                        (first, second) -> {
                                                            throw new FlinkRuntimeException(
                                                                    String.format(
                                                                            MERGE_EXCEPTION_MSG,
                                                                            k));
                                                        }));
                            }
                            return mergedLabels;
                        },
                        mainThreadExecutor);
    }
}
