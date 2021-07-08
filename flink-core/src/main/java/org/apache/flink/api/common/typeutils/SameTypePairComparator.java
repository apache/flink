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
package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;

@Internal
public class SameTypePairComparator<T> extends TypePairComparator<T, T> {

    private TypeComparator<T> comp, comp2;

    public SameTypePairComparator(TypeComparator<T> comp) {
        this.comp = comp;
        comp2 = comp.duplicate();
    }

    @Override
    public void setReference(T reference) {
        comp.setReference(reference);
    }

    @Override
    public boolean equalToReference(T candidate) {
        return comp.equalToReference(candidate);
    }

    @Override
    public int compareToReference(T candidate) {
        comp2.setReference(candidate);
        return comp.compareToReference(comp2);
    }
}
