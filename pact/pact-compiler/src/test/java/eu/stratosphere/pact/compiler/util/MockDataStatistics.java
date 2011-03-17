/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler.util;


import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.compiler.DataStatistics;


/**
 * This class overrides the methods used by the DataStatistics Object to determine statistics and returns pre-defined
 * statistics.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MockDataStatistics extends DataStatistics
{
	private final Map<String, BasicFileStatistics> statsForFiles;
	
	
	public MockDataStatistics() {
		this.statsForFiles = new HashMap<String, DataStatistics.BasicFileStatistics>();
	}
	
	
	public void setStatsForFile(String filePath, BasicFileStatistics stats) {
		this.statsForFiles.put(filePath, stats);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.DataStatistics#getFileStatistics(java.lang.String, eu.stratosphere.pact.common.io.InputFormat)
	 */
	@Override
	public BasicFileStatistics getFileStatistics(String filePath, InputFormat<?, ?> format) {
		return statsForFiles.get(filePath);
	}
	
}
