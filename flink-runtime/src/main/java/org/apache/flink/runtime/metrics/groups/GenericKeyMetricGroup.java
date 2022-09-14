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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;

/**
 * A {@link GenericMetricGroup} for representing the key part of a key-value metric group pair.
 *
 * @see GenericValueMetricGroup
 * @see MetricGroup#addGroup(String, String)
 */
@Internal
public class GenericKeyMetricGroup extends GenericMetricGroup {

    GenericKeyMetricGroup(MetricRegistry registry, AbstractMetricGroup parent, String name) {
        super(registry, parent, name);
    }

    @Override
    public MetricGroup addGroup(String key, String value) {
        return addGroup(key).addGroup(value);
    }

    @Override
    protected GenericMetricGroup createChildGroup(String name, ChildType childType) {
        switch (childType) {
            case VALUE:
                return new GenericValueMetricGroup(registry, this, name);
            default:
                return new GenericMetricGroup(registry, this, name);
        }
    }
}
