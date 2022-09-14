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

package org.apache.flink.testutils.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to use with {@link org.apache.flink.testutils.junit.RetryRule}.
 *
 * <p>Add the {@link org.apache.flink.testutils.junit.RetryRule} to your test class and annotate the
 * class and/or tests with {@link RetryOnException}.
 *
 * <pre>
 * {@literal @}RetryOnException(times=1, exception=IOException.class)
 * public class YourTest {
 *
 *     {@literal @}Rule
 *     public RetryRule retryRule = new RetryRule();
 *
 *     {@literal @}Test
 *     public void yourTest() throws Exception {
 *         // This will be retried 1 time (total runs 2) before failing the test.
 *         throw new IOException("Failing test");
 *     }
 *
 *     {@literal @}Test
 *     {@literal @}RetryOnException(times=2, exception=IOException.class)
 *     public void yourTest() throws Exception {
 *         // This will be retried 2 times (total runs 3) before failing the test.
 *         throw new IOException("Failing test");
 *     }
 *
 *     {@literal @}Test
 *     {@literal @}RetryOnException(times=1, exception=IOException.class)
 *     public void yourTest() throws Exception {
 *         // This will not be retried, because it throws the wrong exception
 *         throw new IllegalStateException("Failing test");
 *     }
 * }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({java.lang.annotation.ElementType.METHOD, ElementType.TYPE})
@Inherited
public @interface RetryOnException {

    int times();

    Class<? extends Throwable> exception();
}
