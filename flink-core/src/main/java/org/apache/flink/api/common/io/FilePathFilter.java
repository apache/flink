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
package org.apache.flink.api.common.io;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;

/**
 * The {@link #filterPath(Path)} method is responsible for deciding if a path is eligible for further
 * processing or not. This can serve to exclude temporary or partial files that
 * are still being written.
 */
@PublicEvolving
public abstract class FilePathFilter implements Serializable {

	private static final long serialVersionUID = 1L;

	/** Name of an unfinished Hadoop file */
	public static final String HADOOP_COPYING = "_COPYING_";

	/**
	 * Returns {@code true} if the {@code filePath} given is to be
	 * ignored when processing a directory, e.g.
	 * <pre>
	 * {@code
	 *
	 * public boolean filterPaths(Path filePath) {
	 *     return filePath.getName().startsWith(".") || filePath.getName().contains("_COPYING_");
	 * }
	 * }</pre>
	 */
	public abstract boolean filterPath(Path filePath);

	/**
	 * Returns the default filter, which excludes the following files:
	 * 
	 * <ul>
	 *     <li>Files starting with &quot;_&quot;</li>
	 *     <li>Files starting with &quot;.&quot;</li>
	 *     <li>Files containing the string &quot;_COPYING_&quot;</li>
	 * </ul>
	 * 
	 * @return The singleton instance of the default file path filter.
	 */
	public static FilePathFilter createDefaultFilter() {
		return DefaultFilter.INSTANCE;
	}

	// ------------------------------------------------------------------------
	//  The default filter
	// ------------------------------------------------------------------------

	/**
	 * The default file path filtering method and is used
	 * if no other such function is provided. This filter leaves out
	 * files starting with ".", "_", and "_COPYING_".
	 */
	public static class DefaultFilter extends FilePathFilter {

		private static final long serialVersionUID = 1L;

		static final DefaultFilter INSTANCE = new DefaultFilter();

		DefaultFilter() {}

		@Override
		public boolean filterPath(Path filePath) {
			return filePath == null ||
				filePath.getName().startsWith(".") ||
				filePath.getName().startsWith("_") ||
				filePath.getName().contains(HADOOP_COPYING);
		}
	}
}
