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

package org.apache.flink.cep.context;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.cep.functions.PatternProcessFunction;

/**
 * Enables access to time related characteristics such as current processing time or timestamp of currently processed
 * element. Used in {@link PatternProcessFunction} and
 * {@link org.apache.flink.cep.pattern.conditions.IterativeCondition}
 */
@PublicEvolving
public interface TimerContext {

	/**
	 * Timestamp of the element currently being processed.
	 *
	 * <p>This might be {@code null}, for example if the time characteristic of your program
	 * is set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
	 */
	Long timestamp();

	/** Returns the current processing time. */
	long currentProcessingTime();

}
