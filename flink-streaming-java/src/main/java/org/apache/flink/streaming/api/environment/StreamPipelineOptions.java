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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.streaming.api.TimeCharacteristic;

/**
 * The {@link ConfigOption configuration options} for job execution. Those are stream specific options.
 * See also {@link org.apache.flink.configuration.PipelineOptions}.
 */
@PublicEvolving
public class StreamPipelineOptions {
	public static final ConfigOption<TimeCharacteristic> TIME_CHARACTERISTIC =
		ConfigOptions.key("pipeline.time-characteristic")
			.enumType(TimeCharacteristic.class)
			.defaultValue(TimeCharacteristic.ProcessingTime)
			.withDescription(Description.builder()
				.text("The time characteristic for all created streams, e.g., processing" +
					"time, event time, or ingestion time.")
				.linebreak()
				.linebreak()
				.text("If you set the characteristic to IngestionTime or EventTime this will set a default " +
					"watermark update interval of 200 ms. If this is not applicable for your application " +
					"you should change it using %s.", TextElement.code(PipelineOptions.AUTO_WATERMARK_INTERVAL.key()))
				.build());
}
