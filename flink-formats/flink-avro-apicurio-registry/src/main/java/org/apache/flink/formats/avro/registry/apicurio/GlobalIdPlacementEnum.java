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

package org.apache.flink.formats.avro.registry.apicurio;

/** Enumeration of where to put the GlobalId. */
enum GlobalIdPlacementEnum {
    HEADER("HEADER", "global ID is put in the header."),
    LEGACY("LEGACY", "global ID is put in the message as a long."),
    CONFLUENT("CONFLUENT", "global ID is put in the message as an int."),
    ;

    private final String value;
    private final String description;

    GlobalIdPlacementEnum(String value, String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String toString() {
        return value;
    }

    public String getDescription() {
        return description;
    }
}
