/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.fs.filemapping;

/** Indicates the ownership of a file in ForSt DB. */
public enum FileOwnership {
    /**
     * The file is privately owned by DB. "Owned by DB" means the file's lifecycle is managed by DB,
     * i.e., it will be deleted when DB decides to dispose of it. "Privately" indicates that its
     * ownership cannot be transferred to JM.
     */
    PRIVATE_OWNED_BY_DB,

    /**
     * The file is owned by DB but is shareable. "Shareable" means its ownership can be transferred
     * to JM in the future.
     */
    SHAREABLE_OWNED_BY_DB,

    /**
     * The file is not owned by DB. That means its lifecycle is not managed by DB, and only JM can
     * decide when to delete it.
     */
    NOT_OWNED
}
