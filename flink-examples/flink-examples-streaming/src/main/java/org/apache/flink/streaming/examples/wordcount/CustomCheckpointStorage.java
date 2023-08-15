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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.runtime.state.CheckpointStorageFactory;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;

import static org.apache.flink.configuration.ConfigOptions.key;

/** A custom checkpoint storage. */
public class CustomCheckpointStorage extends FileSystemCheckpointStorage {
    public CustomCheckpointStorage(String checkpointDirectory) {
        super(checkpointDirectory);
    }

    private static final ConfigOption<String> PATH =
            key("custom-checkpoint-storage.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(new Description.DescriptionBuilder().text("test").build());

    /** A factory to create custom checkpoint storage. */
    public static class CustomCheckpointStorageFactory
            implements CheckpointStorageFactory<CustomCheckpointStorage> {

        @Override
        public CustomCheckpointStorage createFromConfig(
                ReadableConfig config, ClassLoader classLoader)
                throws IllegalConfigurationException {
            return new CustomCheckpointStorage(config.get(PATH));
        }
    }
}
