/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.io;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to indicate the name of io operator.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface IONameAnnotation {
	/**
	 * Name of io operator.
	 *
	 * <p>Through this annotation, it can get all io operator in package.
	 *
	 * <p>With {@link IOTypeAnnotation}, it can get unique io operator.
	 *
	 * @return Name of operator.
	 *
	 * @see IOTypeAnnotation
	 */
	String name();

	/**
	 * Has start time in io operator.
	 *
	 * <p>We can get all io operator which have start time, then get the data from start time to show
	 *
	 * @return
	 */
	boolean start() default false;
}
