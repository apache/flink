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

package org.apache.flink.runtime.metrics.dump;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import akka.actor.UntypedActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.metrics.dump.MetricDumpSerialization.MetricDumpSerializer;

/**
 * The MetricQueryService creates a key-value representation of all metrics currently registered with Flink when queried.
 *
 * <p>It is realized as an actor and can be notified of
 * - an added metric by calling {@link MetricQueryService#notifyOfAddedMetric(ActorRef, Metric, String, AbstractMetricGroup)}
 * - a removed metric by calling {@link MetricQueryService#notifyOfRemovedMetric(ActorRef, Metric)}
 * - a metric dump request by sending the return value of {@link MetricQueryService#getCreateDump()}
 */
public class MetricQueryService extends UntypedActor {
	private static final Logger LOG = LoggerFactory.getLogger(MetricQueryService.class);

	public static final String METRIC_QUERY_SERVICE_NAME = "MetricQueryService";

	private static final CharacterFilter FILTER = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return replaceInvalidChars(input);
		}
	};

	private final MetricDumpSerializer serializer = new MetricDumpSerializer();

	private final Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();
	private final Map<Counter, Tuple2<QueryScopeInfo, String>> counters = new HashMap<>();
	private final Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms = new HashMap<>();
	private final Map<Meter, Tuple2<QueryScopeInfo, String>> meters = new HashMap<>();

	@Override
	public void postStop() {
		serializer.close();
	}

	@Override
	public void onReceive(Object message) {
		try {
			if (message instanceof AddMetric) {
				AddMetric added = (AddMetric) message;

				String metricName = added.metricName;
				Metric metric = added.metric;
				AbstractMetricGroup group = added.group;

				QueryScopeInfo info = group.getQueryServiceMetricInfo(FILTER);

				if (metric instanceof Counter) {
					counters.put((Counter) metric, new Tuple2<>(info, FILTER.filterCharacters(metricName)));
				} else if (metric instanceof Gauge) {
					gauges.put((Gauge<?>) metric, new Tuple2<>(info, FILTER.filterCharacters(metricName)));
				} else if (metric instanceof Histogram) {
					histograms.put((Histogram) metric, new Tuple2<>(info, FILTER.filterCharacters(metricName)));
				} else if (metric instanceof Meter) {
					meters.put((Meter) metric, new Tuple2<>(info, FILTER.filterCharacters(metricName)));
				}
			} else if (message instanceof RemoveMetric) {
				Metric metric = (((RemoveMetric) message).metric);
				if (metric instanceof Counter) {
					this.counters.remove(metric);
				} else if (metric instanceof Gauge) {
					this.gauges.remove(metric);
				} else if (metric instanceof Histogram) {
					this.histograms.remove(metric);
				} else if (metric instanceof Meter) {
					this.meters.remove(metric);
				}
			} else if (message instanceof CreateDump) {
				MetricDumpSerialization.MetricSerializationResult dump = serializer.serialize(counters, gauges, histograms, meters);
				getSender().tell(dump, getSelf());
			} else {
				LOG.warn("MetricQueryServiceActor received an invalid message. " + message.toString());
				getSender().tell(new Status.Failure(new IOException("MetricQueryServiceActor received an invalid message. " + message.toString())), getSelf());
			}
		} catch (Exception e) {
			LOG.warn("An exception occurred while processing a message.", e);
		}
	}

	/**
	 * Lightweight method to replace unsupported characters.
	 * If the string does not contain any unsupported characters, this method creates no
	 * new string (and in fact no new objects at all).
	 *
	 * <p>Replacements:
	 *
	 * <ul>
	 *     <li>{@code space : . ,} are replaced by {@code _} (underscore)</li>
	 * </ul>
	 */
	static String replaceInvalidChars(String str) {
		char[] chars = null;
		final int strLen = str.length();
		int pos = 0;

		for (int i = 0; i < strLen; i++) {
			final char c = str.charAt(i);
			switch (c) {
				case ' ':
				case '.':
				case ':':
				case ',':
					if (chars == null) {
						chars = str.toCharArray();
					}
					chars[pos++] = '_';
					break;
				default:
					if (chars != null) {
						chars[pos] = c;
					}
					pos++;
			}
		}

		return chars == null ? str : new String(chars, 0, pos);
	}

	/**
	 * Starts the MetricQueryService actor in the given actor system.
	 *
	 * @param actorSystem The actor system running the MetricQueryService
	 * @param resourceID resource ID to disambiguate the actor name
	 * @return actor reference to the MetricQueryService
	 */
	public static ActorRef startMetricQueryService(ActorSystem actorSystem, ResourceID resourceID) {
		String actorName = resourceID == null
			? METRIC_QUERY_SERVICE_NAME
			: METRIC_QUERY_SERVICE_NAME + "_" + resourceID.getResourceIdString();
		return actorSystem.actorOf(Props.create(MetricQueryService.class), actorName);
	}

	/**
	 * Utility method to notify a MetricQueryService of an added metric.
	 *
	 * @param service    MetricQueryService to notify
	 * @param metric     added metric
	 * @param metricName metric name
	 * @param group      group the metric was added on
	 */
	public static void notifyOfAddedMetric(ActorRef service, Metric metric, String metricName, AbstractMetricGroup group) {
		service.tell(new AddMetric(metricName, metric, group), null);
	}

	/**
	 * Utility method to notify a MetricQueryService of a removed metric.
	 *
	 * @param service MetricQueryService to notify
	 * @param metric  removed metric
	 */
	public static void notifyOfRemovedMetric(ActorRef service, Metric metric) {
		service.tell(new RemoveMetric(metric), null);
	}

	private static class AddMetric {
		private final String metricName;
		private final Metric metric;
		private final AbstractMetricGroup group;

		private AddMetric(String metricName, Metric metric, AbstractMetricGroup group) {
			this.metricName = metricName;
			this.metric = metric;
			this.group = group;
		}
	}

	private static class RemoveMetric {
		private final Metric metric;

		private RemoveMetric(Metric metric) {
			this.metric = metric;
		}
	}

	public static Object getCreateDump() {
		return CreateDump.INSTANCE;
	}

	private static class CreateDump implements Serializable {
		private static final CreateDump INSTANCE = new CreateDump();
	}
}
