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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Common Flink's options to describe its JVM process memory configuration for JM or TM. */
public class ProcessMemoryOptions {
    private final List<ConfigOption<MemorySize>> requiredFineGrainedOptions;
    private final ConfigOption<MemorySize> totalFlinkMemoryOption;
    private final ConfigOption<MemorySize> totalProcessMemoryOption;
    private final JvmMetaspaceAndOverheadOptions jvmOptions;

    public ProcessMemoryOptions(
            List<ConfigOption<MemorySize>> requiredFineGrainedOptions,
            ConfigOption<MemorySize> totalFlinkMemoryOption,
            ConfigOption<MemorySize> totalProcessMemoryOption,
            JvmMetaspaceAndOverheadOptions jvmOptions) {
        this.requiredFineGrainedOptions = new ArrayList<>(checkNotNull(requiredFineGrainedOptions));
        this.totalFlinkMemoryOption = checkNotNull(totalFlinkMemoryOption);
        this.totalProcessMemoryOption = checkNotNull(totalProcessMemoryOption);
        this.jvmOptions = checkNotNull(jvmOptions);
    }

    List<ConfigOption<MemorySize>> getRequiredFineGrainedOptions() {
        return Collections.unmodifiableList(requiredFineGrainedOptions);
    }

    ConfigOption<MemorySize> getTotalFlinkMemoryOption() {
        return totalFlinkMemoryOption;
    }

    ConfigOption<MemorySize> getTotalProcessMemoryOption() {
        return totalProcessMemoryOption;
    }

    JvmMetaspaceAndOverheadOptions getJvmOptions() {
        return jvmOptions;
    }
}
