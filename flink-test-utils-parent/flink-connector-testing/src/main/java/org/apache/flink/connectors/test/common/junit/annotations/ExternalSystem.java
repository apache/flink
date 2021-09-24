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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the field in test class defining external system.
 *
 * <p>Only one field can be annotated as external system in test class.
 *
 * <p>The lifecycle of external system will be PER-CLASS for performance, because launching and
 * tearing down external system could be relatively a heavy operation.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Experimental
public @interface ExternalSystem {}
