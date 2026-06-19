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

import org.apache.flink.core.fs.Path;

import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * A file filter that filters out hidden files, see {@link DefaultFileFilter} and the files whose
 * path doesn't match the given regex pattern.
 */
public class RegexFileFilter implements Predicate<Path> {
    private final Pattern pattern;
    private final DefaultFileFilter defaultFileFilter;

    public RegexFileFilter(String pathPattern) {
        this.defaultFileFilter = new DefaultFileFilter();
        this.pattern = Pattern.compile(pathPattern);
    }

    @Override
    public boolean test(Path path) {
        return defaultFileFilter.test(path) && pattern.matcher(path.getPath()).matches();
    }
}
