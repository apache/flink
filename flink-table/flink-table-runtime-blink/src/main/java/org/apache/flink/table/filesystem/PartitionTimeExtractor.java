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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

/** Time extractor to extract time from partition values. */
@Experimental
public interface PartitionTimeExtractor extends Serializable {

    String DEFAULT = "default";
    String CUSTOM = "custom";

    /** Extract time from partition keys and values. */
    LocalDateTime extract(List<String> partitionKeys, List<String> partitionValues);

    static PartitionTimeExtractor create(
            ClassLoader userClassLoader,
            String extractorKind,
            String extractorClass,
            String extractorPattern) {
        switch (extractorKind) {
            case DEFAULT:
                return new DefaultPartTimeExtractor(extractorPattern);
            case CUSTOM:
                try {
                    return (PartitionTimeExtractor)
                            userClassLoader.loadClass(extractorClass).newInstance();
                } catch (ClassNotFoundException
                        | IllegalAccessException
                        | InstantiationException e) {
                    throw new RuntimeException(
                            "Can not new instance for custom class from " + extractorClass, e);
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported extractor kind: " + extractorKind);
        }
    }
}
