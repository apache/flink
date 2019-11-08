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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * The WatermarkStrategy defines how to generate {@link Watermark}s in the stream sources.
 * The WatermarkStrategy is a builder/factory for the {@link WatermarkGenerator} that
 * generates the watermarks.
 *
 * <p>This interface is {@link Serializable} because watermark strategies may be shipped
 * to workers during distributed execution.
 */
@Public
public interface WatermarkStrategy<T> extends Serializable {

	/**
	 * Instantiates a WatermarkGenerator that generates watermarks according to this strategy.
	 */
	WatermarkGenerator<T> createWatermarkGenerator();
}
