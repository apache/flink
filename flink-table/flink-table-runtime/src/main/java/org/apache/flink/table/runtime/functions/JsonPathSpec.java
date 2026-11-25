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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.functions.SqlJsonUtils.PathMode;

import org.apache.flink.shaded.com.jayway.jsonpath.JsonPath;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Locale;
import java.util.regex.Matcher;

/**
 * A path of {@code JSON_VALUE}, {@code JSON_QUERY} or {@code JSON_EXISTS}, split into its mode and
 * its JSONPath. For a literal path it is created once, instead of for every record.
 */
@Internal
public final class JsonPathSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    final String pathSpec;
    final PathMode mode;
    final String path;

    // JsonPath isn't serializable, so it is compiled on first use
    @Nullable private transient JsonPath compiled;
    private transient boolean notCompilable;
    // the steps of a path of keys and indexes only, computed on first use
    @Nullable private transient Object[] steps;
    private transient boolean stepsComputed;

    public JsonPathSpec(String pathSpec) {
        this.pathSpec = pathSpec;
        final Matcher matcher = SqlJsonUtils.JSON_PATH_BASE.matcher(pathSpec);
        if (matcher.matches()) {
            this.mode = PathMode.valueOf(matcher.group(1).toUpperCase(Locale.ROOT));
            this.path = matcher.group(2);
        } else {
            this.mode = PathMode.STRICT;
            this.path = pathSpec;
        }
    }

    // null if the path doesn't compile; reading it by its string then fails as before
    @Nullable
    JsonPath compiled() {
        if (compiled == null && !notCompilable) {
            try {
                compiled = JsonPath.compile(path);
            } catch (RuntimeException e) {
                notCompilable = true;
            }
        }
        return compiled;
    }

    // the keys and indexes of the path, or null for any other path, see JsonPathReader#steps
    @Nullable
    public Object[] steps() {
        if (!stepsComputed) {
            steps = JsonPathReader.steps(path);
            stepsComputed = true;
        }
        return steps;
    }
}
