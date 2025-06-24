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

package org.apache.flink.table.runtime.collector;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * A listenable collector for lookup join that can be called when an original record was collected.
 */
@Internal
public abstract class ListenableCollector<T> extends TableFunctionCollector<T> {
    @Nullable private CollectListener<T> collectListener;

    public void setCollectListener(@Nullable CollectListener<T> collectListener) {
        this.collectListener = collectListener;
    }

    protected Optional<CollectListener<T>> getCollectListener() {
        return Optional.ofNullable(collectListener);
    }

    /** An interface can listen on collecting original record. */
    public interface CollectListener<T> {

        /** A callback method when an original record was collected, do nothing by default. */
        default void onCollect(T record) {}
    }
}
