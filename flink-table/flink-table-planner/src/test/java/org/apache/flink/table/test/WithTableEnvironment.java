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

package org.apache.flink.table.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Using this annotation you can inject in the test method:
 *
 * <ul>
 *   <li>{@link TableEnvironment}
 *   <li>{@link StreamExecutionEnvironment} (Java or Scala)
 *   <li>{@link StreamTableEnvironment} (Java or Scala)
 * </ul>
 *
 * <p>The underlying parameter injector will infer automatically the type to use from the signature
 * of the test method.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * @ExtendWith(MiniClusterExtension.class) // Use MiniCluster to start Flink
 * class MyTest {
 *
 *      @Test
 *      @WithTableEnvironment
 *      public void myTest(TableEnvironment tEnv) {
 *          // Use tEnv
 *      }
 * }
 * }</pre>
 *
 * <p>Or at class level:
 *
 * <pre>{@code
 * @ExtendWith(MiniClusterExtension.class) // Use MiniCluster to start Flink
 * @WithTableEnvironment
 * class MyTest {
 *
 *      @Test
 *      public void myTest(TableEnvironment tEnv) {
 *          // Use tEnv
 *      }
 * }
 * }</pre>
 *
 * <p>To use the Table API/DataStream integration:
 *
 * <pre>{@code
 * @ExtendWith(MiniClusterExtension.class) // Use MiniCluster to start Flink
 * class MyTest {
 *
 *      @Test
 *      @WithTableEnvironment
 *      public void myTest(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
 *          // Use env and tEnv
 *      }
 * }
 * }</pre>
 *
 * <p>In case you declare the parameter with the {@link TableEnvironment} type, you can override
 * whether to inject {@link TableEnvironment} or {@link StreamTableEnvironment} by setting {@link
 * #withDataStream()} to {@code true}. Note that if you use {@link #withDataStream()} as {@code
 * true}, the injected {@link StreamTableEnvironment} will always be the Java one, even if your test
 * class is developed with Scala.
 *
 * <p>When using with {@link ParameterizedTest}, make sure the table environment parameter is the
 * last one in the signature.
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(TableEnvironmentParameterResolver.class)
public @interface WithTableEnvironment {

    RuntimeExecutionMode executionMode() default RuntimeExecutionMode.STREAMING;

    boolean withDataStream() default false;
}
