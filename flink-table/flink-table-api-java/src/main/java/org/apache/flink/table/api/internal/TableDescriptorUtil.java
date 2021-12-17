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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import java.util.concurrent.atomic.AtomicInteger;

/** Utilities for working with {@link TableDescriptor}. */
@Internal
class TableDescriptorUtil {

    private static final AtomicInteger uniqueId = new AtomicInteger(0);

    /**
     * Returns an unique name for registering unnamed {@link TableDescriptor descriptors}.
     *
     * <p>IDs cannot be associated with the {@link TableDescriptor} directly as the same descriptor
     * might be registered multiple times (e.g. through {@link
     * TableEnvironment#from(TableDescriptor)}.
     */
    static String getUniqueAnonymousPath() {
        return "Unnamed_TableDescriptor$" + uniqueId.incrementAndGet();
    }

    @VisibleForTesting
    static AtomicInteger getCounter() {
        return uniqueId;
    }

    private TableDescriptorUtil() {}
}
