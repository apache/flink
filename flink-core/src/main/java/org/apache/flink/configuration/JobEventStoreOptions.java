/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Configuration options for the job events. */
@PublicEvolving
public class JobEventStoreOptions {

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            key("job-event.store.write-buffer.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1MB"))
                    .withDescription(
                            "The size of the write buffer of JobEventStore. "
                                    + "The content will be flushed to external file system once the buffer is full");

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Duration> FLUSH_INTERVAL =
            key("job-event.store.write-buffer.flush-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "The flush interval of JobEventStore write buffers. Buffer contents will "
                                    + "be flushed to external file system regularly with regard to this value.");

    private JobEventStoreOptions() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }
}
