/*
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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration object for {@link AkkaRpcService}.
 */
public class AkkaRpcServiceConfiguration {

	private final Time timeout;
	private final long maximumFramesize;
	private final Configuration configuration;

	public AkkaRpcServiceConfiguration(Time timeout, long maximumFramesize, Configuration configuration) {
		checkNotNull(timeout);
		checkArgument(maximumFramesize > 0, "Maximum framesize must be positive.");
		this.timeout = timeout;
		this.maximumFramesize = maximumFramesize;
		this.configuration = configuration;
	}

	public Time getTimeout() {
		return timeout;
	}

	public long getMaximumFramesize() {
		return maximumFramesize;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public static AkkaRpcServiceConfiguration fromConfiguration(Configuration configuration) {
		FiniteDuration duration = AkkaUtils.getTimeout(configuration);
		Time timeout = Time.of(duration.length(), duration.unit());

		long maximumFramesize = AkkaRpcServiceUtils.extractMaximumFramesize(configuration);

		return new AkkaRpcServiceConfiguration(timeout, maximumFramesize, configuration);
	}

	public static AkkaRpcServiceConfiguration defaultConfiguration() {
		return fromConfiguration(new Configuration());
	}

}
