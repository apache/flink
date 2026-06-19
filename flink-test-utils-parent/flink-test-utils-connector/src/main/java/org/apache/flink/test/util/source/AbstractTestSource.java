/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.util.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Abstract base class for test sources that provides default implementations for all Source V2
 * methods. This class fixes the enumerator checkpoint type to {@code Void} for the common case
 * where no checkpoint state is needed.
 *
 * <p>For test cases that require checkpointing enumerator state, extend {@link
 * AbstractTestSourceBase} directly with a custom checkpoint type.
 *
 * @param <T> The type of records produced by this source
 */
@PublicEvolving
public abstract class AbstractTestSource<T> extends AbstractTestSourceBase<T, Void> {

    private static final long serialVersionUID = 1L;

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return VoidSerializer.INSTANCE;
    }
}
