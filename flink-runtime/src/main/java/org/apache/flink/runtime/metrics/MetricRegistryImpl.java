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

package org.apache.flink.runtime.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.groups.ReporterScopedSettings;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.QuadConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A MetricRegistry keeps track of all registered {@link Metric Metrics}. It serves as the
 * connection between {@link MetricGroup MetricGroups} and {@link MetricReporter MetricReporters}.
 */
public class MetricRegistryImpl implements MetricRegistry, AutoCloseableAsync {
    private static final Logger LOG = LoggerFactory.getLogger(MetricRegistryImpl.class);

    private final Object lock = new Object();

    private final List<ReporterAndSettings> reporters;
    private final ScheduledExecutorService reporterScheduledExecutor;
    private final ScheduledExecutorService viewUpdaterScheduledExecutor;

    private final ScopeFormats scopeFormats;
    private final char globalDelimiter;

    private final CompletableFuture<Void> terminationFuture;

    private final long maximumFramesize;

    @Nullable private MetricQueryService queryService;

    @Nullable private RpcService metricQueryServiceRpcService;

    private ViewUpdater viewUpdater;

    private boolean isShutdown;

    public MetricRegistryImpl(MetricRegistryConfiguration config) {
        this(config, Collections.emptyList());
    }

    /** Creates a new MetricRegistry and starts the configured reporter. */
    public MetricRegistryImpl(
            MetricRegistryConfiguration config, Collection<ReporterSetup> reporterConfigurations) {
        this(
                config,
                reporterConfigurations,
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("Flink-Metric-Reporter")),
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("Flink-Metric-View-Updater")));
    }

    @VisibleForTesting
    MetricRegistryImpl(
            MetricRegistryConfiguration config,
            Collection<ReporterSetup> reporterConfigurations,
            ScheduledExecutorService scheduledExecutor) {
        this(config, reporterConfigurations, scheduledExecutor, scheduledExecutor);
    }

    MetricRegistryImpl(
            MetricRegistryConfiguration config,
            Collection<ReporterSetup> reporterConfigurations,
            ScheduledExecutorService reporterScheduledExecutor,
            ScheduledExecutorService viewUpdaterScheduledExecutor) {
        this.maximumFramesize = config.getQueryServiceMessageSizeLimit();
        this.scopeFormats = config.getScopeFormats();
        this.globalDelimiter = config.getDelimiter();
        this.terminationFuture = new CompletableFuture<>();
        this.isShutdown = false;

        // second, instantiate any custom configured reporters
        this.reporters = new ArrayList<>(4);

        this.reporterScheduledExecutor = reporterScheduledExecutor;
        this.viewUpdaterScheduledExecutor = viewUpdaterScheduledExecutor;

        this.queryService = null;
        this.metricQueryServiceRpcService = null;

        if (reporterConfigurations.isEmpty()) {
            // no reporters defined
            // by default, don't report anything
            LOG.info("No metrics reporter configured, no metrics will be exposed/reported.");
        } else {
            for (ReporterSetup reporterSetup : reporterConfigurations) {
                final String namedReporter = reporterSetup.getName();

                try {
                    final MetricReporter reporterInstance = reporterSetup.getReporter();
                    final String className = reporterInstance.getClass().getName();

                    if (reporterInstance instanceof Scheduled) {
                        final Duration period = getConfiguredIntervalOrDefault(reporterSetup);

                        LOG.info(
                                "Periodically reporting metrics in intervals of {} for reporter {} of type {}.",
                                TimeUtils.formatWithHighestUnit(period),
                                namedReporter,
                                className);

                        reporterScheduledExecutor.scheduleWithFixedDelay(
                                new MetricRegistryImpl.ReporterTask((Scheduled) reporterInstance),
                                period.toMillis(),
                                period.toMillis(),
                                TimeUnit.MILLISECONDS);
                    } else {
                        LOG.info(
                                "Reporting metrics for reporter {} of type {}.",
                                namedReporter,
                                className);
                    }

                    String delimiterForReporter =
                            reporterSetup.getDelimiter().orElse(String.valueOf(globalDelimiter));
                    if (delimiterForReporter.length() != 1) {
                        LOG.warn(
                                "Failed to parse delimiter '{}' for reporter '{}', using global delimiter '{}'.",
                                delimiterForReporter,
                                namedReporter,
                                globalDelimiter);
                        delimiterForReporter = String.valueOf(globalDelimiter);
                    }

                    reporters.add(
                            new ReporterAndSettings(
                                    reporterInstance,
                                    new ReporterScopedSettings(
                                            reporters.size(),
                                            delimiterForReporter.charAt(0),
                                            reporterSetup.getFilter(),
                                            reporterSetup.getExcludedVariables(),
                                            reporterSetup.getAdditionalVariables())));
                } catch (Throwable t) {
                    LOG.error(
                            "Could not instantiate metrics reporter {}. Metrics might not be exposed/reported.",
                            namedReporter,
                            t);
                }
            }
        }
    }

    private static Duration getConfiguredIntervalOrDefault(ReporterSetup reporterSetup) {
        final Optional<String> configuredPeriod = reporterSetup.getIntervalSettings();
        Duration period = MetricOptions.REPORTER_INTERVAL.defaultValue();

        if (configuredPeriod.isPresent()) {
            try {
                period = TimeUtils.parseDuration(configuredPeriod.get());
            } catch (Exception e) {
                LOG.error(
                        "Cannot parse report interval from config: "
                                + configuredPeriod
                                + " - please use values like '10 SECONDS' or '500 MILLISECONDS'. "
                                + "Using default reporting interval.");
            }
        }
        return period;
    }

    /**
     * Initializes the MetricQueryService.
     *
     * @param rpcService RpcService to create the MetricQueryService on
     * @param resourceID resource ID used to disambiguate the actor name
     */
    public void startQueryService(RpcService rpcService, ResourceID resourceID) {
        synchronized (lock) {
            Preconditions.checkState(
                    !isShutdown(), "The metric registry has already been shut down.");

            try {
                metricQueryServiceRpcService = rpcService;
                queryService =
                        MetricQueryService.createMetricQueryService(
                                rpcService, resourceID, maximumFramesize);
                queryService.start();
            } catch (Exception e) {
                LOG.warn(
                        "Could not start MetricDumpActor. No metrics will be submitted to the WebInterface.",
                        e);
            }
        }
    }

    /**
     * Returns the rpc service that the {@link MetricQueryService} runs in.
     *
     * @return rpc service of hte MetricQueryService
     */
    @Nullable
    public RpcService getMetricQueryServiceRpcService() {
        return metricQueryServiceRpcService;
    }

    /**
     * Returns the address under which the {@link MetricQueryService} is reachable.
     *
     * @return address of the metric query service
     */
    @Override
    @Nullable
    public String getMetricQueryServiceGatewayRpcAddress() {
        if (queryService != null) {
            return queryService.getSelfGateway(MetricQueryServiceGateway.class).getAddress();
        } else {
            return null;
        }
    }

    @VisibleForTesting
    @Nullable
    MetricQueryServiceGateway getMetricQueryServiceGateway() {
        if (queryService != null) {
            return queryService.getSelfGateway(MetricQueryServiceGateway.class);
        } else {
            return null;
        }
    }

    @Override
    public char getDelimiter() {
        return this.globalDelimiter;
    }

    @VisibleForTesting
    char getDelimiter(int reporterIndex) {
        try {
            return reporters.get(reporterIndex).getSettings().getDelimiter();
        } catch (IndexOutOfBoundsException e) {
            LOG.warn(
                    "Delimiter for reporter index {} not found, returning global delimiter.",
                    reporterIndex);
            return this.globalDelimiter;
        }
    }

    @Override
    public int getNumberReporters() {
        return reporters.size();
    }

    @VisibleForTesting
    public List<MetricReporter> getReporters() {
        return reporters.stream()
                .map(ReporterAndSettings::getReporter)
                .collect(Collectors.toList());
    }

    /**
     * Returns whether this registry has been shutdown.
     *
     * @return true, if this registry was shutdown, otherwise false
     */
    public boolean isShutdown() {
        synchronized (lock) {
            return isShutdown;
        }
    }

    /**
     * Shuts down this registry and the associated {@link MetricReporter}.
     *
     * <p>NOTE: This operation is asynchronous and returns a future which is completed once the
     * shutdown operation has been completed.
     *
     * @return Future which is completed once the {@link MetricRegistryImpl} is shut down.
     */
    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (isShutdown) {
                return terminationFuture;
            } else {
                isShutdown = true;
                final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
                final Time gracePeriod = Time.seconds(1L);

                if (metricQueryServiceRpcService != null) {
                    final CompletableFuture<Void> metricQueryServiceRpcServiceTerminationFuture =
                            metricQueryServiceRpcService.closeAsync();
                    terminationFutures.add(metricQueryServiceRpcServiceTerminationFuture);
                }

                Throwable throwable = null;
                for (ReporterAndSettings reporterAndSettings : reporters) {
                    try {
                        reporterAndSettings.getReporter().close();
                    } catch (Throwable t) {
                        throwable = ExceptionUtils.firstOrSuppressed(t, throwable);
                    }
                }
                reporters.clear();

                if (throwable != null) {
                    terminationFutures.add(
                            FutureUtils.completedExceptionally(
                                    new FlinkException(
                                            "Could not shut down the metric reporters properly.",
                                            throwable)));
                }

                final CompletableFuture<Void> reporterExecutorShutdownFuture =
                        ExecutorUtils.nonBlockingShutdown(
                                gracePeriod.toMilliseconds(),
                                TimeUnit.MILLISECONDS,
                                reporterScheduledExecutor);
                terminationFutures.add(reporterExecutorShutdownFuture);

                final CompletableFuture<Void> viewUpdaterExecutorShutdownFuture =
                        ExecutorUtils.nonBlockingShutdown(
                                gracePeriod.toMilliseconds(),
                                TimeUnit.MILLISECONDS,
                                viewUpdaterScheduledExecutor);

                terminationFutures.add(viewUpdaterExecutorShutdownFuture);

                FutureUtils.completeAll(terminationFutures)
                        .whenComplete(
                                (Void ignored, Throwable error) -> {
                                    if (error != null) {
                                        terminationFuture.completeExceptionally(error);
                                    } else {
                                        terminationFuture.complete(null);
                                    }
                                });

                return terminationFuture;
            }
        }
    }

    @Override
    public ScopeFormats getScopeFormats() {
        return scopeFormats;
    }

    // ------------------------------------------------------------------------
    //  Metrics (de)registration
    // ------------------------------------------------------------------------

    @Override
    public void register(Metric metric, String metricName, AbstractMetricGroup group) {
        synchronized (lock) {
            if (isShutdown()) {
                LOG.warn(
                        "Cannot register metric, because the MetricRegistry has already been shut down.");
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Registering metric {}.{}.",
                            group.getLogicalScope(CharacterFilter.NO_OP_FILTER),
                            metricName);
                }
                if (reporters != null) {
                    forAllReporters(MetricReporter::notifyOfAddedMetric, metric, metricName, group);
                }
                try {
                    if (queryService != null) {
                        queryService.addMetric(metricName, metric, group);
                    }
                } catch (Exception e) {
                    LOG.warn("Error while registering metric: {}.", metricName, e);
                }
                try {
                    if (metric instanceof View) {
                        if (viewUpdater == null) {
                            viewUpdater = new ViewUpdater(viewUpdaterScheduledExecutor);
                        }
                        viewUpdater.notifyOfAddedView((View) metric);
                    }
                } catch (Exception e) {
                    LOG.warn("Error while registering metric: {}.", metricName, e);
                }
            }
        }
    }

    @Override
    public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
        synchronized (lock) {
            if (isShutdown()) {
                LOG.warn(
                        "Cannot unregister metric, because the MetricRegistry has already been shut down.");
            } else {
                if (reporters != null) {
                    forAllReporters(
                            MetricReporter::notifyOfRemovedMetric, metric, metricName, group);
                }
                try {
                    if (queryService != null) {
                        queryService.removeMetric(metric);
                    }
                } catch (Exception e) {
                    LOG.warn("Error while unregistering metric: {}.", metricName, e);
                }
                try {
                    if (metric instanceof View) {
                        if (viewUpdater != null) {
                            viewUpdater.notifyOfRemovedView((View) metric);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Error while unregistering metric: {}", metricName, e);
                }
            }
        }
    }

    @GuardedBy("lock")
    private void forAllReporters(
            QuadConsumer<MetricReporter, Metric, String, MetricGroup> operation,
            Metric metric,
            String metricName,
            AbstractMetricGroup group) {
        for (int i = 0; i < reporters.size(); i++) {
            try {
                ReporterAndSettings reporterAndSettings = reporters.get(i);
                if (reporterAndSettings != null) {
                    final String logicalScope = group.getLogicalScope(CharacterFilter.NO_OP_FILTER);
                    if (!reporterAndSettings
                            .settings
                            .getFilter()
                            .filter(metric, metricName, logicalScope)) {
                        LOG.trace(
                                "Ignoring metric {}.{} for reporter #{} due to filter rules.",
                                logicalScope,
                                metricName,
                                i);
                        continue;
                    }
                    FrontMetricGroup front =
                            new FrontMetricGroup<AbstractMetricGroup<?>>(
                                    reporterAndSettings.getSettings(), group);

                    operation.accept(reporterAndSettings.getReporter(), metric, metricName, front);
                }
            } catch (Exception e) {
                LOG.warn("Error while handling metric: {}.", metricName, e);
            }
        }
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    @Nullable
    MetricQueryService getQueryService() {
        return queryService;
    }

    // ------------------------------------------------------------------------

    /**
     * This task is explicitly a static class, so that it does not hold any references to the
     * enclosing MetricsRegistry instance.
     *
     * <p>This is a subtle difference, but very important: With this static class, the enclosing
     * class instance may become garbage-collectible, whereas with an anonymous inner class, the
     * timer thread (which is a GC root) will hold a reference via the timer task and its enclosing
     * instance pointer. Making the MetricsRegistry garbage collectible makes the java.util.Timer
     * garbage collectible, which acts as a fail-safe to stop the timer thread and prevents resource
     * leaks.
     */
    private static final class ReporterTask extends TimerTask {

        private final Scheduled reporter;

        private ReporterTask(Scheduled reporter) {
            this.reporter = reporter;
        }

        @Override
        public void run() {
            try {
                reporter.report();
            } catch (Throwable t) {
                LOG.warn("Error while reporting metrics", t);
            }
        }
    }

    private static class ReporterAndSettings {

        private final MetricReporter reporter;
        private final ReporterScopedSettings settings;

        private ReporterAndSettings(MetricReporter reporter, ReporterScopedSettings settings) {
            this.reporter = Preconditions.checkNotNull(reporter);
            this.settings = Preconditions.checkNotNull(settings);
        }

        public MetricReporter getReporter() {
            return reporter;
        }

        public ReporterScopedSettings getSettings() {
            return settings;
        }
    }
}
