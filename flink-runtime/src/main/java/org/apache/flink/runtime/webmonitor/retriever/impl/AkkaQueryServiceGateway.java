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

package org.apache.flink.runtime.webmonitor.retriever.impl;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.pattern.Patterns;

import java.util.concurrent.CompletableFuture;

import scala.reflect.ClassTag$;

/**
 * {@link MetricQueryServiceGateway} implementation for Akka based {@link MetricQueryService}.
 */
public class AkkaQueryServiceGateway implements MetricQueryServiceGateway {

	private final ActorRef queryServiceActorRef;

	public AkkaQueryServiceGateway(ActorRef queryServiceActorRef) {
		this.queryServiceActorRef = Preconditions.checkNotNull(queryServiceActorRef);
	}

	@Override
	public CompletableFuture<MetricDumpSerialization.MetricSerializationResult> queryMetrics(Time timeout) {
		return FutureUtils.toJava(
			Patterns.ask(queryServiceActorRef, MetricQueryService.getCreateDump(), timeout.toMilliseconds())
				.mapTo(ClassTag$.MODULE$.apply(MetricDumpSerialization.MetricSerializationResult.class))
		);
	}

	@Override
	public String getAddress() {
		return queryServiceActorRef.path().toString();
	}
}
