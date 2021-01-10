/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.util.CoGroupTaskIterator;
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Collections;

public class NonReusingSortMergeCoGroupIterator<T1, T2> implements CoGroupTaskIterator<T1, T2> {

    private static enum MatchStatus {
        NONE_REMAINED,
        FIRST_REMAINED,
        SECOND_REMAINED,
        FIRST_EMPTY,
        SECOND_EMPTY
    }

    // --------------------------------------------------------------------------------------------

    private MatchStatus matchStatus;

    private Iterable<T1> firstReturn;

    private Iterable<T2> secondReturn;

    private TypePairComparator<T1, T2> comp;

    private NonReusingKeyGroupedIterator<T1> iterator1;

    private NonReusingKeyGroupedIterator<T2> iterator2;

    // --------------------------------------------------------------------------------------------

    public NonReusingSortMergeCoGroupIterator(
            MutableObjectIterator<T1> input1,
            MutableObjectIterator<T2> input2,
            TypeSerializer<T1> serializer1,
            TypeComparator<T1> groupingComparator1,
            TypeSerializer<T2> serializer2,
            TypeComparator<T2> groupingComparator2,
            TypePairComparator<T1, T2> pairComparator) {

        this.comp = pairComparator;

        this.iterator1 = new NonReusingKeyGroupedIterator<T1>(input1, groupingComparator1);
        this.iterator2 = new NonReusingKeyGroupedIterator<T2>(input2, groupingComparator2);
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public Iterable<T1> getValues1() {
        return this.firstReturn;
    }

    @Override
    public Iterable<T2> getValues2() {
        return this.secondReturn;
    }

    @Override
    public boolean next() throws IOException {
        boolean firstEmpty = true;
        boolean secondEmpty = true;

        if (this.matchStatus != MatchStatus.FIRST_EMPTY) {
            if (this.matchStatus == MatchStatus.FIRST_REMAINED) {
                // comparator is still set correctly
                firstEmpty = false;
            } else {
                if (this.iterator1.nextKey()) {
                    this.comp.setReference(this.iterator1.getCurrent());
                    firstEmpty = false;
                }
            }
        }

        if (this.matchStatus != MatchStatus.SECOND_EMPTY) {
            if (this.matchStatus == MatchStatus.SECOND_REMAINED) {
                secondEmpty = false;
            } else {
                if (iterator2.nextKey()) {
                    secondEmpty = false;
                }
            }
        }

        if (firstEmpty && secondEmpty) {
            // both inputs are empty
            return false;
        } else if (firstEmpty && !secondEmpty) {
            // input1 is empty, input2 not
            this.firstReturn = Collections.emptySet();
            this.secondReturn = this.iterator2.getValues();
            this.matchStatus = MatchStatus.FIRST_EMPTY;
            return true;
        } else if (!firstEmpty && secondEmpty) {
            // input1 is not empty, input 2 is empty
            this.firstReturn = this.iterator1.getValues();
            this.secondReturn = Collections.emptySet();
            this.matchStatus = MatchStatus.SECOND_EMPTY;
            return true;
        } else {
            // both inputs are not empty
            final int comp = this.comp.compareToReference(this.iterator2.getCurrent());

            if (0 == comp) {
                // keys match
                this.firstReturn = this.iterator1.getValues();
                this.secondReturn = this.iterator2.getValues();
                this.matchStatus = MatchStatus.NONE_REMAINED;
            } else if (0 < comp) {
                // key1 goes first
                this.firstReturn = this.iterator1.getValues();
                this.secondReturn = Collections.emptySet();
                this.matchStatus = MatchStatus.SECOND_REMAINED;
            } else {
                // key 2 goes first
                this.firstReturn = Collections.emptySet();
                this.secondReturn = this.iterator2.getValues();
                this.matchStatus = MatchStatus.FIRST_REMAINED;
            }
            return true;
        }
    }
}
