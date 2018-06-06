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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.akka.ActorUtils;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A MetricRegistry keeps track of all registered {@link Metric Metrics}. It serves as the
 * connection between {@link MetricGroup MetricGroups} and {@link MetricReporter MetricReporters}.
 */
public class MetricRegistryImpl implements MetricRegistry {
	static final Logger LOG = LoggerFactory.getLogger(MetricRegistryImpl.class);

	private final Object lock = new Object();

	private final List<MetricReporter> reporters;
	private final ScheduledExecutorService executor;

	private final ScopeFormats scopeFormats;
	private final char globalDelimiter;
	private final List<Character> delimiters;

	private final CompletableFuture<Void> terminationFuture;

	@Nullable
	private ActorRef queryService;

	@Nullable
	private String metricQueryServicePath;

	private ViewUpdater viewUpdater;

	private boolean isShutdown;

	/**
	 * Creates a new MetricRegistry and starts the configured reporter.
	 */
	public MetricRegistryImpl(MetricRegistryConfiguration config) {
		this.scopeFormats = config.getScopeFormats();
		this.globalDelimiter = config.getDelimiter();
		this.delimiters = new ArrayList<>(10);
		this.terminationFuture = new CompletableFuture<>();
		this.isShutdown = false;

		// second, instantiate any custom configured reporters
		this.reporters = new ArrayList<>(4);

		List<Tuple2<String, Configuration>> reporterConfigurations = config.getReporterConfigurations();

		this.executor = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("Flink-MetricRegistry"));

		this.queryService = null;
		this.metricQueryServicePath = null;

		if (reporterConfigurations.isEmpty()) {
			// no reporters defined
			// by default, don't report anything
			LOG.info("No metrics reporter configured, no metrics will be exposed/reported.");
		} else {
			// we have some reporters so
			for (Tuple2<String, Configuration> reporterConfiguration: reporterConfigurations) {
				String namedReporter = reporterConfiguration.f0;
				Configuration reporterConfig = reporterConfiguration.f1;

				final String className = reporterConfig.getString(ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, null);
				if (className == null) {
					LOG.error("No reporter class set for reporter " + namedReporter + ". Metrics might not be exposed/reported.");
					continue;
				}

				try {
					String configuredPeriod = reporterConfig.getString(ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, null);
					TimeUnit timeunit = TimeUnit.SECONDS;
					long period = 10;

					if (configuredPeriod != null) {
						try {
							String[] interval = configuredPeriod.split(" ");
							period = Long.parseLong(interval[0]);
							timeunit = TimeUnit.valueOf(interval[1]);
						}
						catch (Exception e) {
							LOG.error("Cannot parse report interval from config: " + configuredPeriod +
									" - please use values like '10 SECONDS' or '500 MILLISECONDS'. " +
									"Using default reporting interval.");
						}
					}

					Class<?> reporterClass = Class.forName(className);
					MetricReporter reporterInstance = (MetricReporter) reporterClass.newInstance();

					MetricConfig metricConfig = new MetricConfig();
					reporterConfig.addAllToProperties(metricConfig);
					LOG.info("Configuring {} with {}.", namedReporter, metricConfig);
					reporterInstance.open(metricConfig);

					if (reporterInstance instanceof Scheduled) {
						LOG.info("Periodically reporting metrics in intervals of {} {} for reporter {} of type {}.", period, timeunit.name(), namedReporter, className);

						executor.scheduleWithFixedDelay(
								new MetricRegistryImpl.ReporterTask((Scheduled) reporterInstance), period, period, timeunit);
					} else {
						LOG.info("Reporting metrics for reporter {} of type {}.", namedReporter, className);
					}
					reporters.add(reporterInstance);

					String delimiterForReporter = reporterConfig.getString(ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, String.valueOf(globalDelimiter));
					if (delimiterForReporter.length() != 1) {
						LOG.warn("Failed to parse delimiter '{}' for reporter '{}', using global delimiter '{}'.", delimiterForReporter, namedReporter, globalDelimiter);
						delimiterForReporter = String.valueOf(globalDelimiter);
					}
					this.delimiters.add(delimiterForReporter.charAt(0));
				}
				catch (Throwable t) {
					LOG.error("Could not instantiate metrics reporter {}. Metrics might not be exposed/reported.", namedReporter, t);
				}
			}
		}
	}

	/**
	 * Initializes the MetricQueryService.
	 *
	 * @param actorSystem ActorSystem to create the MetricQueryService on
	 * @param resourceID resource ID used to disambiguate the actor name
     */
	public void startQueryService(ActorSystem actorSystem, ResourceID resourceID) {
		synchronized (lock) {
			Preconditions.checkState(!isShutdown(), "The metric registry has already been shut down.");

			try {
				queryService = MetricQueryService.startMetricQueryService(actorSystem, resourceID);
				metricQueryServicePath = AkkaUtils.getAkkaURL(actorSystem, queryService);
			} catch (Exception e) {
				LOG.warn("Could not start MetricDumpActor. No metrics will be submitted to the WebInterface.", e);
			}
		}
	}

	/**
	 * Returns the address under which the {@link MetricQueryService} is reachable.
	 *
	 * @return address of the metric query service
	 */
	@Override
	@Nullable
	public String getMetricQueryServicePath() {
		return metricQueryServicePath;
	}

	@Override
	public char getDelimiter() {
		return this.globalDelimiter;
	}

	@Override
	public char getDelimiter(int reporterIndex) {
		try {
			return delimiters.get(reporterIndex);
		} catch (IndexOutOfBoundsException e) {
			LOG.warn("Delimiter for reporter index {} not found, returning global delimiter.", reporterIndex);
			return this.globalDelimiter;
		}
	}

	@Override
	public int getNumberReporters() {
		return reporters.size();
	}

	@VisibleForTesting
	public List<MetricReporter> getReporters() {
		return reporters;
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
	 * <p>NOTE: This operation is asynchronous and returns a future which is completed
	 * once the shutdown operation has been completed.
	 *
	 * @return Future which is completed once the {@link MetricRegistryImpl}
	 * is shut down.
	 */
	public CompletableFuture<Void> shutdown() {
		synchronized (lock) {
			if (isShutdown) {
				return terminationFuture;
			} else {
				isShutdown = true;
				final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
				final Time gracePeriod = Time.seconds(1L);

				if (queryService != null) {
					final CompletableFuture<Void> queryServiceTerminationFuture = ActorUtils.nonBlockingShutDown(
						gracePeriod.toMilliseconds(),
						TimeUnit.MILLISECONDS,
						queryService);

					terminationFutures.add(queryServiceTerminationFuture);
				}

				Throwable throwable = null;
				for (MetricReporter reporter : reporters) {
					try {
						reporter.close();
					} catch (Throwable t) {
						throwable = ExceptionUtils.firstOrSuppressed(t, throwable);
					}
				}
				reporters.clear();

				if (throwable != null) {
					terminationFutures.add(
						FutureUtils.completedExceptionally(
							new FlinkException("Could not shut down the metric reporters properly.", throwable)));
				}

				final CompletableFuture<Void> executorShutdownFuture = ExecutorUtils.nonBlockingShutdown(
					gracePeriod.toMilliseconds(),
					TimeUnit.MILLISECONDS,
					executor);

				terminationFutures.add(executorShutdownFuture);

				FutureUtils
					.completeAll(terminationFutures)
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
				LOG.warn("Cannot register metric, because the MetricRegistry has already been shut down.");
			} else {
				if (reporters != null) {
					for (int i = 0; i < reporters.size(); i++) {
						MetricReporter reporter = reporters.get(i);
						try {
							if (reporter != null) {
								FrontMetricGroup front = new FrontMetricGroup<AbstractMetricGroup<?>>(i, group);
								reporter.notifyOfAddedMetric(metric, metricName, front);
							}
						} catch (Exception e) {
							LOG.warn("Error while registering metric.", e);
						}
					}
				}
				try {
					if (queryService != null) {
						MetricQueryService.notifyOfAddedMetric(queryService, metric, metricName, group);
					}
				} catch (Exception e) {
					LOG.warn("Error while registering metric.", e);
				}
				try {
					if (metric instanceof View) {
						if (viewUpdater == null) {
							viewUpdater = new ViewUpdater(executor);
						}
						viewUpdater.notifyOfAddedView((View) metric);
					}
				} catch (Exception e) {
					LOG.warn("Error while registering metric.", e);
				}
			}
		}
	}

	@Override
	public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
		synchronized (lock) {
			if (isShutdown()) {
				LOG.warn("Cannot unregister metric, because the MetricRegistry has already been shut down.");
			} else {
				if (reporters != null) {
					for (int i = 0; i < reporters.size(); i++) {
						try {
						MetricReporter reporter = reporters.get(i);
							if (reporter != null) {
								FrontMetricGroup front = new FrontMetricGroup<AbstractMetricGroup<?>>(i, group);
								reporter.notifyOfRemovedMetric(metric, metricName, front);
							}
						} catch (Exception e) {
							LOG.warn("Error while registering metric.", e);
						}
					}
				}
				try {
					if (queryService != null) {
						MetricQueryService.notifyOfRemovedMetric(queryService, metric);
					}
				} catch (Exception e) {
					LOG.warn("Error while registering metric.", e);
				}
				try {
					if (metric instanceof View) {
						if (viewUpdater != null) {
							viewUpdater.notifyOfRemovedView((View) metric);
						}
					}
				} catch (Exception e) {
					LOG.warn("Error while registering metric.", e);
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	@VisibleForTesting
	@Nullable
	public ActorRef getQueryService() {
		return queryService;
	}

	// ------------------------------------------------------------------------

	/**
	 * This task is explicitly a static class, so that it does not hold any references to the enclosing
	 * MetricsRegistry instance.
	 *
	 * <p>This is a subtle difference, but very important: With this static class, the enclosing class instance
	 * may become garbage-collectible, whereas with an anonymous inner class, the timer thread
	 * (which is a GC root) will hold a reference via the timer task and its enclosing instance pointer.
	 * Making the MetricsRegistry garbage collectible makes the java.util.Timer garbage collectible,
	 * which acts as a fail-safe to stop the timer thread and prevents resource leaks.
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
}
