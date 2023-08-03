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
import org.apache.flink.api.common.io.NonParallelInput;

import java.io.Serializable;
import java.util.Iterator;

/**
 * An input format that returns objects from an iterator.
 *
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@PublicEvolving
public class IteratorInputFormat<T> extends GenericInputFormat<T> implements NonParallelInput {

    private static final long serialVersionUID = 1L;

    private Iterator<T> iterator; // input data as serializable iterator

    public IteratorInputFormat(Iterator<T> iterator) {
        if (!(iterator instanceof Serializable)) {
            throw new IllegalArgumentException("The data source iterator must be serializable.");
        }

        this.iterator = iterator;
    }

    @Override
    public boolean reachedEnd() {
        return !this.iterator.hasNext();
    }

    @Override
    public T nextRecord(T record) {
        return this.iterator.next();
    }
}
