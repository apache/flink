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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;

import java.util.Collections;
import java.util.Set;

/** Base interface for {@link DecodingFormatFactory} and {@link EncodingFormatFactory}. */
@PublicEvolving
public interface FormatFactory extends Factory {

    /**
     * Returns a set of {@link ConfigOption} that are directly forwarded to the runtime
     * implementation but don't affect the final execution topology.
     *
     * <p>Options declared here can override options of the persisted plan during an enrichment
     * phase. Since a restored topology is static, an implementer has to ensure that the declared
     * options don't affect fundamental abilities such as {@link ChangelogMode}.
     *
     * <p>For example, given a JSON format, if an option defines how to parse timestamps, changing
     * the parsing behavior does not affect the pipeline topology and can be allowed. However, an
     * option that defines whether the format results in a {@link ProjectableDecodingFormat} or not
     * is not allowed. The wrapping connector and planner might not react to the changed abilities
     * anymore.
     *
     * @see DynamicTableFactory.Context#getEnrichmentOptions()
     */
    default Set<ConfigOption<?>> forwardOptions() {
        return Collections.emptySet();
    }
}
