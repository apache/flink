/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.common.io.statistics.BaseStatistics;

/**
 * The collection of access methods that can be used to retrieve statistical information about the
 * data processed in a job. Currently this method acts as an entry point only for obtaining cached
 * statistics.
 */
public class DataStatistics {
	
	private final Map<String, BaseStatistics> baseStatisticsCache;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new statistics object, with an empty cache. 
	 */
	public DataStatistics() {
		this.baseStatisticsCache = new HashMap<String, BaseStatistics>();
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the base statistics for the input identified by the given identifier.
	 *  
	 * @param inputIdentifier The identifier for the input.
	 * @return The statistics that were cached for this input.
	 */
	public BaseStatistics getBaseStatistics(String inputIdentifier) {
		synchronized (this.baseStatisticsCache) {
			return this.baseStatisticsCache.get(inputIdentifier);
		}
	}
	
	/**
	 * Caches the given statistics. They are later retrievable under the given identifier.
	 * 
	 * @param statistics The statistics to cache.
	 * @param identifyer The identifier which may be later used to retrieve the statistics.
	 */
	public void cacheBaseStatistics(BaseStatistics statistics, String identifyer) {
		synchronized (this.baseStatisticsCache) {
			this.baseStatisticsCache.put(identifyer, statistics);
		}
	}
}
