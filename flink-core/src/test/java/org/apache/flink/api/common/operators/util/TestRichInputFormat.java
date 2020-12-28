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

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;

import java.io.IOException;

/** Same as the non rich test input format, except it provide access to runtime context. */
public class TestRichInputFormat extends GenericInputFormat<String> implements NonParallelInput {

    private static final long serialVersionUID = 1L;
    private static final int NUM = 5;
    private static final String[] NAMES = TestIOData.NAMES;
    private int count = 0;
    private boolean openCalled = false;
    private boolean closeCalled = false;

    @Override
    public boolean reachedEnd() throws IOException {
        return count >= NUM;
    }

    @Override
    public String nextRecord(String reuse) throws IOException {
        count++;
        return NAMES[count - 1]
                + getRuntimeContext().getIndexOfThisSubtask()
                + ""
                + getRuntimeContext().getNumberOfParallelSubtasks();
    }

    public void reset() {
        count = 0;
        openCalled = false;
        closeCalled = false;
    }

    @Override
    public void openInputFormat() {
        openCalled = true;
    }

    @Override
    public void closeInputFormat() {
        closeCalled = true;
    }

    public boolean hasBeenOpened() {
        return openCalled;
    }

    public boolean hasBeenClosed() {
        return closeCalled;
    }
}
