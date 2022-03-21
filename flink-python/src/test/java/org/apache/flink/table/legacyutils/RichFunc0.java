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

package org.apache.flink.table.legacyutils;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import org.junit.Assert;

/**
 * Testing scalar function to verify that lifecycle methods are called in the expected order and
 * only once.
 */
public class RichFunc0 extends ScalarFunction {
    private static final long serialVersionUID = 931156471687322386L;

    private boolean openCalled = false;
    private boolean closeCalled = false;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        if (openCalled) {
            Assert.fail("Open called more than once.");
        } else {
            openCalled = true;
        }
        if (closeCalled) {
            Assert.fail("Close called before open.");
        }
    }

    public void eval(int index) {
        if (!openCalled) {
            Assert.fail("Open was not called before eval.");
        }
        if (closeCalled) {
            Assert.fail("Close called before eval.");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (closeCalled) {
            Assert.fail("Close called more than once.");
        } else {
            closeCalled = true;
        }
        if (!openCalled) {
            Assert.fail("Open was not called before close.");
        }
    }
}
