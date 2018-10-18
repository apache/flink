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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import scala.concurrent.duration.FiniteDuration;

/**
 * Configuration object for {@link AkkaRpcService}.
 */
public class AkkaRpcServiceConfiguration {

	private static final String SIMPLE_AKKA_CONFIG_TEMPLATE =
		"akka {remote {netty.tcp {maximum-frame-size = %s}}}";

	private static final String MAXIMUM_FRAME_SIZE_PATH =
		"akka.remote.netty.tcp.maximum-frame-size";

	private final Time timeout;
	private final long maximumFramesize;

	public AkkaRpcServiceConfiguration(Time timeout, long maximumFramesize) {
		this.timeout = timeout;
		this.maximumFramesize = maximumFramesize;
	}

	public Time getTimeout() {
		return timeout;
	}

	public long getMaximumFramesize() {
		return maximumFramesize;
	}

	public static AkkaRpcServiceConfiguration fromConfiguration(Configuration configuration) {
		FiniteDuration duration = AkkaUtils.getTimeout(configuration);
		Time timeout = Time.of(duration.length(), duration.unit());

		String maxFrameSizeStr = configuration.getString(AkkaOptions.FRAMESIZE);
		String akkaConfigStr = String.format(SIMPLE_AKKA_CONFIG_TEMPLATE, maxFrameSizeStr);
		Config akkaConfig = ConfigFactory.parseString(akkaConfigStr);
		long maximumFramesize = akkaConfig.getBytes(MAXIMUM_FRAME_SIZE_PATH);

		return new AkkaRpcServiceConfiguration(timeout, maximumFramesize);
	}

	public static AkkaRpcServiceConfiguration defaultConfiguration() {
		return fromConfiguration(new Configuration());
	}

}
