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

import org.apache.flink.configuration.MemorySize;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** JVM metaspace and overhead memory sizes. */
public class JvmMetaspaceAndOverhead implements Serializable {
    private static final long serialVersionUID = 1L;

    private final MemorySize metaspace;
    private final MemorySize overhead;

    public JvmMetaspaceAndOverhead(MemorySize jvmMetaspace, MemorySize jvmOverhead) {
        this.metaspace = checkNotNull(jvmMetaspace);
        this.overhead = checkNotNull(jvmOverhead);
    }

    MemorySize getTotalJvmMetaspaceAndOverheadSize() {
        return getMetaspace().add(getOverhead());
    }

    public MemorySize getMetaspace() {
        return metaspace;
    }

    public MemorySize getOverhead() {
        return overhead;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof JvmMetaspaceAndOverhead) {
            JvmMetaspaceAndOverhead that = (JvmMetaspaceAndOverhead) obj;
            return Objects.equals(this.metaspace, that.metaspace)
                    && Objects.equals(this.overhead, that.overhead);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metaspace, overhead);
    }
}
