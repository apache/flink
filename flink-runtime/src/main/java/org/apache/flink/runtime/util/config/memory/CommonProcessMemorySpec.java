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

import java.util.Objects;

/**
 * Common memory components of Flink processes (e.g. JM or TM).
 *
 * <p>The process memory consists of the following components.
 *
 * <ul>
 *   <li>Total Flink Memory
 *   <li>JVM Metaspace
 *   <li>JVM Overhead
 * </ul>
 *
 * Among all the components, We use the Total Process Memory to refer to all the memory components,
 * while the Total Flink Memory refers to all the internal components except JVM Metaspace and JVM
 * Overhead. The internal components of Total Flink Memory, represented by {@link FlinkMemory}, are
 * specific to concrete Flink process (e.g. JM or TM).
 *
 * <p>The relationships of process memory components are shown below.
 *
 * <pre>
 *               ┌ ─ ─ Total Process Memory  ─ ─ ┐
 *               │┌─────────────────────────────┐│
 *                │      Total Flink Memory     │
 *               │└─────────────────────────────┘│
 *               │┌─────────────────────────────┐│
 *                │        JVM Metaspace        │
 *               │└─────────────────────────────┘│
 *                ┌─────────────────────────────┐
 *               ││        JVM Overhead         ││
 *                └─────────────────────────────┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
public class CommonProcessMemorySpec<FM extends FlinkMemory> implements ProcessMemorySpec {
    private static final long serialVersionUID = 1L;

    private final FM flinkMemory;
    private final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead;

    protected CommonProcessMemorySpec(
            FM flinkMemory, JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
        this.flinkMemory = flinkMemory;
        this.jvmMetaspaceAndOverhead = jvmMetaspaceAndOverhead;
    }

    public FM getFlinkMemory() {
        return flinkMemory;
    }

    public JvmMetaspaceAndOverhead getJvmMetaspaceAndOverhead() {
        return jvmMetaspaceAndOverhead;
    }

    @Override
    public MemorySize getJvmHeapMemorySize() {
        return flinkMemory.getJvmHeapMemorySize();
    }

    @Override
    public MemorySize getJvmDirectMemorySize() {
        return flinkMemory.getJvmDirectMemorySize();
    }

    public MemorySize getJvmMetaspaceSize() {
        return getJvmMetaspaceAndOverhead().getMetaspace();
    }

    public MemorySize getJvmOverheadSize() {
        return getJvmMetaspaceAndOverhead().getOverhead();
    }

    @Override
    public MemorySize getTotalFlinkMemorySize() {
        return flinkMemory.getTotalFlinkMemorySize();
    }

    public MemorySize getTotalProcessMemorySize() {
        return flinkMemory
                .getTotalFlinkMemorySize()
                .add(getJvmMetaspaceSize())
                .add(getJvmOverheadSize());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && getClass().equals(obj.getClass())) {
            CommonProcessMemorySpec<?> that = (CommonProcessMemorySpec<?>) obj;
            return Objects.equals(this.flinkMemory, that.flinkMemory)
                    && Objects.equals(this.jvmMetaspaceAndOverhead, that.jvmMetaspaceAndOverhead);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(flinkMemory, jvmMetaspaceAndOverhead);
    }
}
