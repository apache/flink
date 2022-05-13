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

package org.apache.flink.runtime.persistence;

import java.io.Serializable;

/**
 * Resource version for specific state handle on the underlying storage. The implementation also
 * needs to implement the {@link Comparable} interface so that we could compare the resource
 * versions.
 *
 * @param <R> Type of {@link ResourceVersion}
 */
public interface ResourceVersion<R> extends Comparable<R>, Serializable {

    /**
     * Check whether the state handle is existing.
     *
     * @return true if state handle exists with current {@link ResourceVersion} on external storage.
     *     Or false it does not exist.
     */
    boolean isExisting();
}
