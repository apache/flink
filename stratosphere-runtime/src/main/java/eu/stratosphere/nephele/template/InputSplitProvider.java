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

package eu.stratosphere.nephele.template;

import eu.stratosphere.core.io.InputSplit;

/**
 * An input split provider can be successively queried to provide a series of {@link InputSplit} objects an
 * {@link AbstractInputTask} is supposed to consume in the course of its execution.
 * 
 * @author warneke
 */
public interface InputSplitProvider {

	/**
	 * Requests the next input split to be consumed by the calling {@link AbstractInputTask}.
	 * 
	 * @return the next input split to be consumed by the calling {@link AbstractInputTask} or <code>null</code> if the
	 *         {@link AbstractInputTask} shall not consume any further input splits.
	 */
	InputSplit getNextInputSplit();
}
