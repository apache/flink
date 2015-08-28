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

package org.apache.flink.streaming.api.windowing.helper;

import java.io.Serializable;

import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.KeepAllEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * Window that represents the full stream history. Can be used only as eviction
 * policy and only with operations that support pre-aggregator such as reduce or
 * aggregations.
 */
public class FullStream<DATA> extends WindowingHelper<DATA> implements Serializable {

	private static final long serialVersionUID = 1L;

	private FullStream() {
	}

	@Override
	public EvictionPolicy<DATA> toEvict() {
		return new KeepAllEvictionPolicy<DATA>();
	}

	@Override
	public TriggerPolicy<DATA> toTrigger() {
		throw new RuntimeException(
				"Full stream policy can be only used as eviction. Use .every(..) after the window call.");
	}

	/**
	 * Returns a helper representing an eviction that keeps all previous record
	 * history.
	 */
	public static <R> FullStream<R> window() {
		return new FullStream<R>();
	}

}
