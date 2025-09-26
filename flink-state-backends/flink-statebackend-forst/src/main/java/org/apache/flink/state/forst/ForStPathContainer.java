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

package org.apache.flink.state.forst;

import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** Container for ForSt paths. */
public class ForStPathContainer {

    private static final Logger LOG = LoggerFactory.getLogger(ForStResourceContainer.class);
    public static final String DB_DIR_STRING = "db";

    @Nullable final Path localJobPath;
    @Nullable private final Path localBasePath;
    @Nullable private final Path localForStPath;

    @Nullable private final Path remoteJobPath;
    @Nullable private final Path remoteBasePath;
    @Nullable private final Path remoteForStPath;

    public static ForStPathContainer empty() {
        return of(null, null, null, null);
    }

    public static ForStPathContainer ofLocal(
            @Nullable Path localJobPath, @Nullable Path localBasePath) {
        return new ForStPathContainer(localJobPath, localBasePath, null, null);
    }

    public static ForStPathContainer of(
            @Nullable Path localJobPath,
            @Nullable Path localBasePath,
            @Nullable Path remoteJobPath,
            @Nullable Path remoteBasePath) {
        return new ForStPathContainer(localJobPath, localBasePath, remoteJobPath, remoteBasePath);
    }

    public ForStPathContainer(
            @Nullable Path localJobPath,
            @Nullable Path localBasePath,
            @Nullable Path remoteJobPath,
            @Nullable Path remoteBasePath) {
        this.localJobPath = localJobPath;
        this.localBasePath = localBasePath;
        this.localForStPath = localBasePath != null ? new Path(localBasePath, DB_DIR_STRING) : null;

        this.remoteJobPath = remoteJobPath;
        this.remoteBasePath = remoteBasePath;
        this.remoteForStPath =
                remoteBasePath != null ? new Path(remoteBasePath, DB_DIR_STRING) : null;

        LOG.info(
                "ForStPathContainer: localJobPath: {}, localBasePath: {}, localForStPath:{},  remoteJobPath: {}, remoteBasePath: {}, remoteForStPath: {}",
                localJobPath,
                localBasePath,
                localForStPath,
                remoteJobPath,
                remoteBasePath,
                remoteForStPath);
    }

    public @Nullable Path getLocalJobPath() {
        return localJobPath;
    }

    public @Nullable Path getLocalBasePath() {
        return localBasePath;
    }

    public @Nullable Path getLocalForStPath() {
        return localForStPath;
    }

    public @Nullable Path getRemoteJobPath() {
        return remoteJobPath;
    }

    public @Nullable Path getRemoteBasePath() {
        return remoteBasePath;
    }

    public @Nullable Path getRemoteForStPath() {
        return remoteForStPath;
    }

    public Path getJobPath() {
        if (remoteJobPath != null) {
            return remoteJobPath;
        } else {
            return localJobPath;
        }
    }

    public Path getBasePath() {
        if (remoteBasePath != null) {
            return remoteBasePath;
        } else {
            return localBasePath;
        }
    }

    public Path getDbPath() {
        if (remoteForStPath != null) {
            return remoteForStPath;
        } else {
            return localForStPath;
        }
    }

    @Override
    public String toString() {
        return "ForStPathContainer(localJobPath = ["
                + localJobPath
                + "] localBasePath = ["
                + localBasePath
                + "] localForStPath = ["
                + localForStPath
                + "] remoteJobPath = ["
                + remoteJobPath
                + "] remoteBasePath = ["
                + remoteBasePath
                + "] remoteForStPath = ["
                + remoteForStPath
                + "])";
    }
}
