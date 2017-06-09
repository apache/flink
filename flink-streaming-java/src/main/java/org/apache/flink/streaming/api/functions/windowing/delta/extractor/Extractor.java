/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.windowing.delta.extractor;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * Extractors allow to extract/convert one type to another. They are mostly used
 * to extract some fields out of a more complex structure (Tuple/Array) to run
 * further calculation on the extraction result.
 *
 * @param <FROM>
 *            The input data type.
 * @param <TO>
 *            The output data type.
 */
@Internal
public interface Extractor<FROM, TO> extends Serializable {

	/**
	 * Extracts/Converts the given input to an object of the output type.
	 *
	 * @param in
	 *            the input data
	 * @return the extracted/converted data
	 */
	TO extract(FROM in);

}
