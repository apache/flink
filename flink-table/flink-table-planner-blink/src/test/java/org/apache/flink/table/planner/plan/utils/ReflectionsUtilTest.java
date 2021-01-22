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

package org.apache.flink.table.planner.plan.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/** Test for {@link ReflectionsUtil}. */
@RunWith(Parameterized.class)
public class ReflectionsUtilTest {

    @Parameterized.Parameter public boolean includingInterface;

    @Parameterized.Parameter(1)
    public boolean includingAbstractClass;

    @Test
    public void testScanSubClasses() {
        Set<Class<? extends TestInterface>> actual =
                ReflectionsUtil.scanSubClasses(
                        ReflectionsUtilTest.class.getPackage().getName(),
                        TestInterface.class,
                        includingInterface,
                        includingAbstractClass);
        Set<Class<? extends TestInterface>> expected = new HashSet<>();
        expected.add(TestClass1.class);
        expected.add(TestClass2.class);
        expected.add(TestClass3.class);
        if (includingInterface) {
            expected.add(TestSubInterface.class);
        }
        if (includingAbstractClass) {
            expected.add(TestAbstractClass.class);
        }
        assertEquals(expected, actual);
    }

    @Parameterized.Parameters(name = "includingInterface={0}, includingAbstractClass={1}")
    public static Object[][] testData() {
        return new Object[][] {
            new Object[] {false, false},
            new Object[] {true, false},
            new Object[] {false, true},
            new Object[] {true, true}
        };
    }

    /** Testing interface. */
    public interface TestInterface {}

    /** Testing interface. */
    public interface TestSubInterface extends TestInterface {}

    /** Testing abstract class. */
    public abstract static class TestAbstractClass implements TestSubInterface {}

    /** Testing class. */
    public static class TestClass1 implements TestInterface {}

    /** Testing class. */
    public static class TestClass2 implements TestSubInterface {}

    /** Testing class. */
    public static class TestClass3 extends TestAbstractClass {}
}
