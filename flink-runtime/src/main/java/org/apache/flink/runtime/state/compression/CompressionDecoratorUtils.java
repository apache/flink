/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.compression;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.util.DynamicCodeLoadingException;

import javax.annotation.Nullable;

/**
 * Utility class to work with {@link StreamCompressionDecorator}
 */
public class CompressionDecoratorUtils {

	/**
	 * Returns the target compression decorator with provided one or instantiated from configuration,
	 * or just return null if nothing specified.
	 *
	 * @param streamCompressionDecorator The given streamCompressionDecorator, maybe null.
	 * @param className The class name configured in {@link CheckpointingOptions#COMPRESSION_DECORATOR}.
	 * @param classLoader The class loader.
	 * @return the target compression decorator with provided one or instantiated from configuration,
	 *  or just return null if nothing specified.
	 * @throws DynamicCodeLoadingException
	 *             Thrown if a class name is configured and the compression decorator class was not
	 *             found or the compression decorator could not be instantiated.
	 */
	@Nullable
	public static StreamCompressionDecorator loadCompressionDecorator(
			@Nullable StreamCompressionDecorator streamCompressionDecorator,
			@Nullable String className,
			ClassLoader classLoader) throws DynamicCodeLoadingException {

		if (streamCompressionDecorator != null) {
			return streamCompressionDecorator;
		}

		if (className == null || className.isEmpty()) {
			return null;
		}

		try {
			@SuppressWarnings("rawtypes")
			Class<? extends StreamCompressionDecorator> clazz =
				Class.forName(className, false, classLoader)
					.asSubclass(StreamCompressionDecorator.class);

			return clazz.newInstance();
		} catch (ClassNotFoundException e) {
			throw new DynamicCodeLoadingException(
				"Cannot find configured stream compression decorator class: " + className, e);
		} catch (ClassCastException | InstantiationException | IllegalAccessException e) {
			throw new DynamicCodeLoadingException("The class configured under '" + CheckpointingOptions.COMPRESSION_DECORATOR.key() +
				"' is not a valid stream compression decorator (" + className + ')', e);
		}
	}
}
