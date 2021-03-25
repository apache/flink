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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/** {@link ConfigOptions} for {@link FsStateChangelogWriterFactory}. */
@Experimental
@Documentation.ExcludeFromDocumentation
public class FsStateChangelogOptions {
    public static final ConfigOption<String> BASE_PATH =
            ConfigOptions.key("dstl.dfs.base-path").stringType().noDefaultValue();
    public static final ConfigOption<Boolean> BATCH_ENABLED =
            ConfigOptions.key("dstl.dfs.batch.enabled").booleanType().defaultValue(true);
    public static final ConfigOption<Long> PERSIST_DELAY_MS =
            ConfigOptions.key("dstl.dfs.batch.persist-delay-ms").longType().defaultValue(50L);
    public static final ConfigOption<MemorySize> PERSIST_SIZE_THRESHOLD =
            ConfigOptions.key("dstl.dfs.batch.persist-size-threshold-kb")
                    .memoryType()
                    .defaultValue(MemorySize.parse("100Kb"));
}
