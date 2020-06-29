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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;

import java.util.function.Predicate;

/**
 * A file filter that filters out hidden files based on common naming patterns,
 * i.e., files where the filename starts with '.' or with '_'.
 */
@PublicEvolving
public final class DefaultFileFilter implements Predicate<Path> {

	@Override
	public boolean test(Path path) {
		final String fileName = path.getName();
		if (fileName == null || fileName.length() == 0) {
			return true;
		}
		final char first = fileName.charAt(0);
		return first != '.' && first != '_';
	}
}
