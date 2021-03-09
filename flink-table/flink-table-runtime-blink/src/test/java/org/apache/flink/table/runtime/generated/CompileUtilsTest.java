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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/** Tests for {@link CompileUtils}. */
public class CompileUtilsTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        // cleanup cached class before tests
        CompileUtils.COMPILED_CACHE.invalidateAll();
    }

    @Test
    public void testCacheReuse() {
        String code = "public class Main {\n" + "  int i;\n" + "  int j;\n" + "}";

        Class<?> class1 = CompileUtils.compile(this.getClass().getClassLoader(), "Main", code);
        Class<?> class2 = CompileUtils.compile(this.getClass().getClassLoader(), "Main", code);
        Class<?> class3 = CompileUtils.compile(new TestClassLoader(), "Main", code);
        assertSame(class1, class2);
        assertNotSame(class1, class3);
    }

    @Test
    public void testWrongCode() {
        String code = "public class111 Main {\n" + "  int i;\n" + "  int j;\n" + "}";

        thrown.expect(FlinkRuntimeException.class);
        CompileUtils.compile(this.getClass().getClassLoader(), "Main", code);
    }

    private static class TestClassLoader extends URLClassLoader {

        TestClassLoader() {
            super(new URL[0], Thread.currentThread().getContextClassLoader());
        }
    }
}
