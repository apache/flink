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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.util.SplittableIterator;

import java.io.IOException;
import java.util.Iterator;

/** An input format that generates data in parallel through a {@link SplittableIterator}. */
@PublicEvolving
public class ParallelIteratorInputFormat<T> extends GenericInputFormat<T> {

    private static final long serialVersionUID = 1L;

    private final SplittableIterator<T> source;

    private transient Iterator<T> splitIterator;

    public ParallelIteratorInputFormat(SplittableIterator<T> iterator) {
        this.source = iterator;
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);

        this.splitIterator =
                this.source.getSplit(split.getSplitNumber(), split.getTotalNumberOfSplits());
    }

    @Override
    public boolean reachedEnd() {
        return !this.splitIterator.hasNext();
    }

    @Override
    public T nextRecord(T reuse) {
        return this.splitIterator.next();
    }
}
