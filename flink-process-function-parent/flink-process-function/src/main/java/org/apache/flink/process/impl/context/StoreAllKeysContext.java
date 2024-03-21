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

package org.apache.flink.process.impl.context;

import java.util.HashSet;
import java.util.Iterator;

/** This {@link AllKeysContext} will listen and store all keys it has seen. */
public class StoreAllKeysContext implements AllKeysContext {
    /** Used to store all the keys seen by this input. */
    private final HashSet<Object> allKeys = new HashSet<>();

    @Override
    public void onKeySelected(Object newKey) {
        allKeys.add(newKey);
    }

    /** Get the iterator of all keys. */
    public Iterator<Object> getAllKeysIter() {
        return allKeys.iterator();
    }
}
