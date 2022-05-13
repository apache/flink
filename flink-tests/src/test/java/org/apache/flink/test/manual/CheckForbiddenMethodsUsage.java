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

package org.apache.flink.test.manual;

import org.apache.flink.types.parser.FieldParserTest;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.MemberUsageScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests via reflection that certain methods are not called in Flink.
 *
 * <p>Forbidden calls include: - Byte / String conversions that do not specify an explicit charset
 * because they produce different results in different locales
 */
@Ignore("broken test; see FLINK-21340")
public class CheckForbiddenMethodsUsage {

    private static class ForbiddenCall {

        private final Method method;
        private final Constructor<?> constructor;
        private final List<Member> exclusions;

        private ForbiddenCall(Method method, Constructor<?> ctor, List<Member> exclusions) {
            this.method = method;
            this.exclusions = exclusions;
            this.constructor = ctor;
        }

        public Method getMethod() {
            return method;
        }

        public List<Member> getExclusions() {
            return exclusions;
        }

        public Set<Member> getUsages(Reflections reflections) {
            if (method == null) {
                return reflections.getConstructorUsage(constructor);
            }

            return reflections.getMethodUsage(method);
        }

        public static ForbiddenCall of(Method method) {
            return new ForbiddenCall(method, null, Collections.<Member>emptyList());
        }

        public static ForbiddenCall of(Method method, List<Member> exclusions) {
            return new ForbiddenCall(method, null, exclusions);
        }

        public static ForbiddenCall of(Constructor<?> ctor) {
            return new ForbiddenCall(null, ctor, Collections.<Member>emptyList());
        }

        public static ForbiddenCall of(Constructor<?> ctor, List<Member> exclusions) {
            return new ForbiddenCall(null, ctor, exclusions);
        }
    }

    // ------------------------------------------------------------------------

    private static final List<ForbiddenCall> forbiddenCalls = new ArrayList<>();

    @BeforeClass
    public static void init() throws Exception {
        forbiddenCalls.add(
                ForbiddenCall.of(
                        String.class.getMethod("getBytes"),
                        Arrays.<Member>asList(
                                FieldParserTest.class.getMethod("testEndsWithDelimiter"),
                                FieldParserTest.class.getMethod("testDelimiterNext"))));
        forbiddenCalls.add(ForbiddenCall.of(String.class.getConstructor(byte[].class)));
        forbiddenCalls.add(ForbiddenCall.of(String.class.getConstructor(byte[].class, int.class)));
        forbiddenCalls.add(
                ForbiddenCall.of(String.class.getConstructor(byte[].class, int.class, int.class)));
        forbiddenCalls.add(
                ForbiddenCall.of(
                        String.class.getConstructor(
                                byte[].class, int.class, int.class, int.class)));
    }

    @Test
    public void testNoDefaultEncoding() throws Exception {
        final Reflections reflections =
                new Reflections(
                        new ConfigurationBuilder()
                                .useParallelExecutor(Runtime.getRuntime().availableProcessors())
                                .addUrls(ClasspathHelper.forPackage("org.apache.flink"))
                                .addScanners(new MemberUsageScanner()));

        for (ForbiddenCall forbiddenCall : forbiddenCalls) {
            final Set<Member> methodUsages = forbiddenCall.getUsages(reflections);
            methodUsages.removeAll(forbiddenCall.getExclusions());
            assertEquals("Unexpected calls: " + methodUsages, 0, methodUsages.size());
        }
    }
}
