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
package org.apache.flink.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.flink.annotation.Internal;

/**
 * A class that specifies a group of {@link ConfigOption}. Name of the group will be converted into resulting html
 * filename by changing camel case into underscore notation and suffix _configuration.html. {@link ConfigOption}.
 * See also {@link ConfigGroups}
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Internal
public @interface ConfigGroup {
	String name();

	String keyPrefix() default "";
}
