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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.drivers.parameter.Simplify.Ordering;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link Simplify}. */
public class SimplifyTest extends ParameterTestBase {

    private Simplify parameter;

    @Before
    public void setup() {
        super.setup();

        parameter = new Simplify(owner);
    }

    @Test
    public void testWithDirected() {
        parameter.configure(ParameterTool.fromArgs(new String[] {"--simplify", "directed"}));
        Assert.assertEquals(Ordering.DIRECTED, parameter.getValue());
    }

    @Test
    public void testWithUndirected() {
        parameter.configure(ParameterTool.fromArgs(new String[] {"--simplify", "undirected"}));
        Assert.assertEquals(Ordering.UNDIRECTED, parameter.getValue());
    }

    @Test
    public void testWithNoParameter() {
        parameter.configure(ParameterTool.fromArgs(new String[] {}));
        Assert.assertEquals(Ordering.NONE, parameter.getValue());
    }
}
