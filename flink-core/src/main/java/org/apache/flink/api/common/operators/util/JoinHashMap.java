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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.apache.commons.collections4.map.AbstractHashedMap;

@Internal
public class JoinHashMap<BT> extends AbstractHashedMap {

    private final TypeSerializer<BT> buildSerializer;

    private final TypeComparator<BT> buildComparator;

    public JoinHashMap(TypeSerializer<BT> buildSerializer, TypeComparator<BT> buildComparator) {
        super(64);
        this.buildSerializer = buildSerializer;
        this.buildComparator = buildComparator;
    }

    public TypeSerializer<BT> getBuildSerializer() {
        return buildSerializer;
    }

    public TypeComparator<BT> getBuildComparator() {
        return buildComparator;
    }

    public <PT> Prober<PT> createProber(
            TypeComparator<PT> probeComparator, TypePairComparator<PT, BT> pairComparator) {
        return new Prober<>(probeComparator, pairComparator);
    }

    @SuppressWarnings("unchecked")
    public void insertOrReplace(BT record) {
        Object key = buildComparator.hash(record);
        buildComparator.setReference(record);
        if (containsKey(key)) {
            HashEntry<Object, BT> entry = getEntry(key);
            if (entry != null) {
                if (buildComparator.equalToReference(entry.getValue())) {
                    entry.setValue(record);
                    return;
                }
            }
        }
        put(key, record);
    }

    public class Prober<PT> {

        public Prober(
                TypeComparator<PT> probeComparator, TypePairComparator<PT, BT> pairComparator) {
            this.probeComparator = probeComparator;
            this.pairComparator = pairComparator;
        }

        private final TypeComparator<PT> probeComparator;

        private final TypePairComparator<PT, BT> pairComparator;

        @SuppressWarnings("unchecked")
        public BT lookupMatch(PT record) {
            Object key = probeComparator.hash(record);
            pairComparator.setReference(record);
            if (containsKey(key)) {
                HashEntry<Object, PT> entry = getEntry(key);
                if (entry != null) {
                    if (pairComparator.equalToReference((BT) entry.getValue())) {
                        return (BT) entry.setValue(record);
                    }
                }
            }
            return null;
        }
    }
}
