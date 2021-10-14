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

package org.apache.flink.connectors.test.common.junit.annotations;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.connectors.test.common.external.ExternalContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the field in test class defining a {@link ExternalContext.Factory} for constructing {@link
 * ExternalContext} before invocation of each test case.
 *
 * <p>Multiple fields can be annotated as external context factory, and these external contexts will
 * be provided as different parameters of test cases.
 *
 * <p>The lifecycle of a {@link ExternalContext} will be PER-CASE, which means an instance of {@link
 * ExternalContext} will be constructed before invocation of each test case, and closed right after
 * the execution of the case for isolation between test cases.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Experimental
public @interface ExternalContextFactory {}
