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

package org.apache.flink.runtime.rest.versioning;

import java.util.Collection;
import java.util.Collections;

/** Interface for all versions of the REST API. */
public interface RestAPIVersion<T extends RestAPIVersion<T>> extends Comparable<T> {
    /**
     * Returns the URL version prefix (e.g. "v1") for this version.
     *
     * @return URL version prefix
     */
    String getURLVersionPrefix();

    /**
     * Returns whether this version is the default REST API version.
     *
     * @return whether this version is the default
     */
    boolean isDefaultVersion();

    /**
     * Returns whether this version is considered stable.
     *
     * @return whether this version is stable
     */
    boolean isStableVersion();

    /**
     * Accept versions and one of them as a comparator, and get the latest one.
     *
     * @return latest version that implement RestAPIVersion interface>
     */
    static <E extends RestAPIVersion<E>> E getLatestVersion(Collection<E> versions) {
        return Collections.max(versions);
    }
}
