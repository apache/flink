/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.compiler.plan;

/**
 * Enumeration to indicate the mode of temporarily materializing the data that flows across a connection.
 * Introducing such an artificial dam is sometimes necessary to avoid that a certain data flows deadlock
 * themselves, or as a cache to replay an intermediate result.
 */
public enum TempMode {
	
	NONE(false, false),
	PIPELINE_BREAKER(false, true),
	CACHED(true, false),
	CACHING_PIPELINE_BREAKER(true, true);
	
	// --------------------------------------------------------------------------------------------
	
	private final boolean cached;
	
	private final boolean breaksPipeline;
	
	
	private TempMode(boolean cached, boolean breaksPipeline) {
		this.cached = cached;
		this.breaksPipeline = breaksPipeline;
	}

	public boolean isCached() {
		return cached;
	}

	public boolean breaksPipeline() {
		return breaksPipeline;
	}
	
	public TempMode makePipelineBreaker() {
		if (this == NONE) {
			return PIPELINE_BREAKER;
		} else if (this == CACHED) {
			return CACHING_PIPELINE_BREAKER;
		} else {
			return this;
		}
	}
	
	public TempMode makeCached() {
		if (this == NONE) {
			return CACHED;
		} else if (this == PIPELINE_BREAKER) {
			return CACHING_PIPELINE_BREAKER;
		} else {
			return this;
		}
	}
}