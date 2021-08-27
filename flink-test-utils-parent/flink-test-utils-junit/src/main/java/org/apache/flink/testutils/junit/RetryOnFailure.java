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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to use with {@link RetryRule}.
 *
 * <p>Add the {@link RetryRule} to your test class and annotate the class and/or tests with {@link
 * RetryOnFailure}.
 *
 * <pre>
 * {@literal @}RetryOnFailure(times=1)
 * public class YourTest {
 *
 *     {@literal @}Rule
 *     public RetryRule retryRule = new RetryRule();
 *
 *     {@literal @}Test
 *     public void yourTest() {
 *         // This will be retried 1 time (total runs 2) before failing the test.
 *         throw new Exception("Failing test");
 *     }
 *
 *     {@literal @}Test
 *     {@literal @}RetryOnFailure(times=2)
 *     public void yourTest() {
 *         // This will be retried 2 time (total runs 3) before failing the test.
 *         throw new Exception("Failing test");
 *     }
 * }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({java.lang.annotation.ElementType.METHOD, ElementType.TYPE})
public @interface RetryOnFailure {
    int times();
}
