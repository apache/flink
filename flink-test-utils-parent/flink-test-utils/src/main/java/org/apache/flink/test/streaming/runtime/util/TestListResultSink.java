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

package org.apache.flink.test.streaming.runtime.util;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Thread-safe sink for collecting elements into an on-heap list.
 *
 * @param <T> element type
 */
public class TestListResultSink<T> extends RichSinkFunction<T> {

    private static final long serialVersionUID = 1L;
    private int resultListId;

    public TestListResultSink() {
        this.resultListId = TestListWrapper.getInstance().createList();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
    }

    @Override
    public void invoke(T value) throws Exception {
        synchronized (resultList()) {
            resultList().add(value);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @SuppressWarnings("unchecked")
    private List<T> resultList() {
        synchronized (TestListWrapper.getInstance()) {
            return (List<T>) TestListWrapper.getInstance().getList(resultListId);
        }
    }

    public List<T> getResult() {
        synchronized (resultList()) {
            ArrayList<T> copiedList = new ArrayList<T>(resultList());
            return copiedList;
        }
    }

    public List<T> getSortedResult() {
        synchronized (resultList()) {
            ArrayList<T> sortedList = new ArrayList<T>(resultList());
            Collections.sort((List) sortedList);
            return sortedList;
        }
    }
}
