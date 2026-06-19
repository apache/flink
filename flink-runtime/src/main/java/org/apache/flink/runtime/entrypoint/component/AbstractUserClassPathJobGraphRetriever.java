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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/** Abstract class for the JobGraphRetriever which supports getting user classpaths. */
public abstract class AbstractUserClassPathJobGraphRetriever implements JobGraphRetriever {

    /** User classpaths in relative form to the working directory. */
    @Nonnull private final Collection<URL> userClassPaths;

    protected AbstractUserClassPathJobGraphRetriever(@Nullable File jobDir) throws IOException {
        if (jobDir == null) {
            userClassPaths = Collections.emptyList();
        } else {
            final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
            final Collection<URL> relativeJarURLs =
                    FileUtils.listFilesInDirectory(jobDir.toPath(), FileUtils::isJarFile).stream()
                            .map(path -> FileUtils.relativizePath(workingDirectory, path))
                            .map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
                            .collect(Collectors.toList());
            this.userClassPaths = Collections.unmodifiableCollection(relativeJarURLs);
        }
    }

    protected Collection<URL> getUserClassPaths() {
        return userClassPaths;
    }
}
