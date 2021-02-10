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

package org.apache.flink.table.module;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/** A POJO to represent a module's name and use status. */
@PublicEvolving
public class ModuleEntry {
    private final String name;
    private final boolean used;

    public ModuleEntry(String name, boolean used) {
        this.name = name;
        this.used = used;
    }

    public String name() {
        return name;
    }

    public boolean used() {
        return used;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ModuleEntry entry = (ModuleEntry) o;

        return new EqualsBuilder().append(used, entry.used).append(name, entry.name).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(name).append(used).toHashCode();
    }

    @Override
    public String toString() {
        return "ModuleEntry{" + "name='" + name + '\'' + ", used=" + used + '}';
    }
}
