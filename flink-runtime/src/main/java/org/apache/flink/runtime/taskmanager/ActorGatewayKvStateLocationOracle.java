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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobmaster.KvStateLocationOracle;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateMessage;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

/**
 * {@link KvStateLocationOracle} implementation for {@link ActorGateway}.
 */
public class ActorGatewayKvStateLocationOracle implements KvStateLocationOracle {

	private final ActorGateway jobManagerActorGateway;

	private final FiniteDuration timeout;

	public ActorGatewayKvStateLocationOracle(
			ActorGateway jobManagerActorGateway,
			Time timeout) {
		this.jobManagerActorGateway = Preconditions.checkNotNull(jobManagerActorGateway);

		Preconditions.checkNotNull(timeout);
		this.timeout = FiniteDuration.apply(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	@Override
	public CompletableFuture<KvStateLocation> requestKvStateLocation(JobID jobId, String registrationName) {
		final KvStateMessage.LookupKvStateLocation lookupKvStateLocation = new KvStateMessage.LookupKvStateLocation(jobId, registrationName);

		return FutureUtils.toJava(
			jobManagerActorGateway
				.ask(lookupKvStateLocation, timeout)
				.mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation.class)));
	}
}
