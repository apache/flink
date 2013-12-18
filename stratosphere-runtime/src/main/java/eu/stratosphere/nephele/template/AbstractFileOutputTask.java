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

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;

/**
 * Specialized subtype of {@link AbstractOutputTask} for tasks which are supposed to write output to
 * a file.
 * 
 * @author warneke
 */
public abstract class AbstractFileOutputTask extends AbstractOutputTask {

	/**
	 * Returns the output path which has been assigned to the original {@link JobFileOutputVertex}.
	 * 
	 * @return the output path which has been assigned to the original {@link JobFileOutputVertex} or <code>null</code>
	 *         if the path cannot be retrieved
	 */
	public Path getFileOutputPath() {

		// TODO: This is a quick workaround, problem can be solved in a more generic way
		final Configuration conf = getEnvironment().getTaskConfiguration();

		final String outputPath = conf.getString("outputPath", null);

		if (outputPath != null) {
			return new Path(outputPath);
		}

		return null;
	}

}
