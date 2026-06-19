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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.stream.IntStream;

/** Serializable version of {@link ChangelogMode}. */
@Internal
public class RuntimeChangelogMode implements Serializable {

    private final byte[] changes;
    private final boolean keyOnlyDeletes;

    public RuntimeChangelogMode(byte[] changes, boolean keyOnlyDeletes) {
        this.changes = changes;
        this.keyOnlyDeletes = keyOnlyDeletes;
    }

    public static RuntimeChangelogMode serialize(ChangelogMode mode) {
        final RowKind[] kinds = mode.getContainedKinds().toArray(RowKind[]::new);
        final byte[] changes = new byte[kinds.length];
        IntStream.range(0, kinds.length).forEach(i -> changes[i] = kinds[i].toByteValue());
        return new RuntimeChangelogMode(changes, mode.keyOnlyDeletes());
    }

    public ChangelogMode deserialize() {
        final ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (byte change : changes) {
            builder.addContainedKind(RowKind.fromByteValue(change));
        }
        builder.keyOnlyDeletes(keyOnlyDeletes);
        return builder.build();
    }
}
