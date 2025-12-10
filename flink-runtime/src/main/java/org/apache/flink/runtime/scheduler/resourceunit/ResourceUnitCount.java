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

package org.apache.flink.runtime.scheduler.resourceunit;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/** The class is used to represent the load weight abstraction. */
@Internal
public interface ResourceUnitCount extends Comparable<ResourceUnitCount>, Serializable {

    /**
     * Returns the resource unit count.
     *
     * @return the current resource unit count
     */
    float getCount();

    /**
     * Returns the resource unit count as an integer, truncated if necessary.
     *
     * @return the current resource unit count
     */
    int getCountAsInt();

    /**
     * Merge the other resource unit count and this one into a new object.
     *
     * @param other A loading weight object.
     * @return the new merged {@link ResourceUnitCount}.
     */
    ResourceUnitCount merge(ResourceUnitCount other);
}
