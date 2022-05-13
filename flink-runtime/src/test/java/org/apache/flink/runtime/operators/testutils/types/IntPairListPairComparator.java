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

package org.apache.flink.runtime.operators.testutils.types;

import org.apache.flink.api.common.typeutils.TypePairComparator;

public class IntPairListPairComparator extends TypePairComparator<IntPair, IntList> {

    private int key;

    @Override
    public void setReference(IntPair reference) {
        this.key = reference.getKey();
    }

    @Override
    public boolean equalToReference(IntList candidate) {
        return this.key == candidate.getKey();
    }

    @Override
    public int compareToReference(IntList candidate) {
        return this.key - candidate.getKey();
    }
}
