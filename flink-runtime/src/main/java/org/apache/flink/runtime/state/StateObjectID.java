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
package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * Unique {@link StateObject} identifier used to track shared state. The meaning of sharing depends
 * on the context. For example, state can be shared across:
 *
 * <ul>
 *   <li>different incremental checkpoints
 *   <li>different subtasks after rescaling
 *   <li>different state backends if writing to the same file for optimization
 * </ul>
 *
 * The identifier can be either logical, such as local file name or physical, such as file name on
 * DFS (the distinction is purely semantic). Which one should be used depends on state ownership
 * model:
 *
 * <ul>
 *   <li>with JM-owned state, logical ID MUST be used. Physical object might change because of
 *       re-uploading state from if the previous checkpoint was not-confirmed or was aborted. JM
 *       will need to assign the correct physical state object for the given logical ID; TMs should
 *       not be aware of it
 *   <li>with a mixed state ownership model (when JM only tracks the state shared across multiple
 *       subtasks/backends) physical identifiers must be used. The responsibility of JM is only to
 *       remove those specific physical objects when they are no longer in use.
 *   <li>with pure TM-owned state (though not implemented), any identifiers can be used
 * </ul>
 */
@Internal
public interface StateObjectID extends Serializable {
    static StateObjectID of(String string) {
        return new StringBasedStateObjectID(string);
    }
}
