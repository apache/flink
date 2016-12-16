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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Class for determining if a particular file should be included or excluded
 * based on a set of include and exclude glob filters.
 *
 * Glob filter support the following expressions:
 * <ul>
 *     <li>* - matches any number of any characters including none</li>
 *     <li>** - matches any file in all subdirectories</li>
 *     <li>? - matches any single character</li>
 *     <li>[abc] - matches one of the characters listed in a brackets</li>
 *     <li>[a-z] - matches one character from the range given in the brackets</li>
 * </ul>
 *
 * <p> If does not match an include pattern it is excluded. If it matches and include
 * pattern but also matches an exclude pattern it is excluded.
 *
 * <p> If no patterns are provided all files are included
 */
@Internal
public class GlobFilePathFilter extends FilePathFilter {

	private static final long serialVersionUID = 1L;

	private final List<PathMatcher> includeMatchers;
	private final List<PathMatcher> excludeMatchers;

	/**
	 * Constructor for GlobFilePathFilter that will match all files
	 */
	public GlobFilePathFilter() {
		this(Collections.<String>emptyList(), Collections.<String>emptyList());
	}

	/**
	 * Constructor for GlobFilePathFilter
	 *
	 * @param includePatterns glob patterns for files to include
	 * @param excludePatterns glob patterns for files to exclude
	 */
	public GlobFilePathFilter(List<String> includePatterns, List<String> excludePatterns) {
		includeMatchers = buildPatterns(includePatterns);
		excludeMatchers = buildPatterns(excludePatterns);
	}

	private List<PathMatcher> buildPatterns(List<String> patterns) {
		FileSystem fileSystem = FileSystems.getDefault();
		List<PathMatcher> matchers = new ArrayList<>();

		for (String patternStr : patterns) {
			matchers.add(fileSystem.getPathMatcher("glob:" + patternStr));
		}

		return matchers;
	}

	@Override
	public boolean filterPath(Path filePath) {
		if (includeMatchers.isEmpty() && excludeMatchers.isEmpty()) {
			return false;
		}

		for (PathMatcher matcher : includeMatchers) {
			if (matcher.matches(Paths.get(filePath.getPath()))) {
				return shouldExclude(filePath);
			}
		}

		return true;
	}

	private boolean shouldExclude(Path filePath) {
		for (PathMatcher matcher : excludeMatchers) {
			if (matcher.matches(Paths.get(filePath.getPath()))) {
				return true;
			}
		}
		return false;
	}

}
