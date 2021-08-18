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

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.BASE_PATH;
import static org.apache.flink.configuration.CheckpointingOptions.STATE_CHANGE_LOG_STORAGE;

/** {@link FsStateChangelogStorage} factory. */
@Internal
public class FsStateChangelogStorageFactory implements StateChangelogStorageFactory {

    public static final String IDENTIFIER = "filesystem";

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public StateChangelogStorage<?> createStorage(Configuration configuration) throws IOException {
        return new FsStateChangelogStorage(configuration);
    }

    public static void configure(Configuration configuration, File newFolder) {
        configuration.setString(STATE_CHANGE_LOG_STORAGE, IDENTIFIER);
        configuration.setString(BASE_PATH, newFolder.getAbsolutePath());
    }
}
