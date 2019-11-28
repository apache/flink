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

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Configuration for the {@link AkkaRpcService}.
 */
public class AkkaRpcServiceConfiguration {

	@Nonnull
	private final Configuration configuration;

	@Nonnull
	private final Time timeout;

	private final long maximumFramesize;

	public AkkaRpcServiceConfiguration(@Nonnull Configuration configuration, @Nonnull Time timeout, long maximumFramesize) {
		checkArgument(maximumFramesize > 0L, "Maximum framesize must be positive.");
		this.configuration = configuration;
		this.timeout = timeout;
		this.maximumFramesize = maximumFramesize;
	}

	@Nonnull
	public Configuration getConfiguration() {
		return configuration;
	}

	@Nonnull
	public Time getTimeout() {
		return timeout;
	}

	public long getMaximumFramesize() {
		return maximumFramesize;
	}

	public static AkkaRpcServiceConfiguration fromConfiguration(Configuration configuration) {
		final Time timeout = AkkaUtils.getTimeoutAsTime(configuration);

		final long maximumFramesize = AkkaRpcServiceUtils.extractMaximumFramesize(configuration);

		return new AkkaRpcServiceConfiguration(configuration, timeout, maximumFramesize);
	}

	public static AkkaRpcServiceConfiguration defaultConfiguration() {
		return fromConfiguration(new Configuration());
	}

}
