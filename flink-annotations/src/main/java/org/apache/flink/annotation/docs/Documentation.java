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

package org.apache.flink.annotation.docs;

import org.apache.flink.annotation.Internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Collection of annotations to modify the behavior of the documentation generators.
 */
public final class Documentation {

	/**
	 * Annotation used on config option fields to override the documented default.
	 */
	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@Internal
	public @interface OverrideDefault {
		String value();
	}

	/**
	 * Annotation used on config option fields to include them in the "Common Options" section.
	 *
	 * <p>The {@link CommonOption#position()} argument controls the position in the generated table, with lower values
	 * being placed at the top. Fields with the same position are sorted alphabetically by key.
	 */
	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@Internal
	public @interface CommonOption {
		int POSITION_MEMORY = 10;
		int POSITION_PARALLELISM_SLOTS = 20;
		int POSITION_FAULT_TOLERANCE = 30;
		int POSITION_HIGH_AVAILABILITY = 40;
		int POSITION_SECURITY = 50;

		int position() default Integer.MAX_VALUE;
	}

	/**
	 * Annotation used on table config options for adding meta data labels.
	 *
	 * <p>The {@link TableOption#execMode()} argument indicates the execution mode the config works for
	 * (batch, streaming or both).
	 */
	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@Internal
	public @interface TableOption {
		ExecMode execMode();
	}

	/**
	 * The execution mode the config works for.
	 */
	public enum ExecMode {

		BATCH("Batch"), STREAMING("Streaming"), BATCH_STREAMING("Batch and Streaming");

		private final String name;

		ExecMode(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	/**
	 * Annotation used on config option fields to exclude the config option from documentation.
	 */
	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@Internal
	public @interface ExcludeFromDocumentation {
		/**
		 * The optional reason why the config option is excluded from documentation.
		 */
		String value() default "";
	}

	private Documentation(){
	}
}
