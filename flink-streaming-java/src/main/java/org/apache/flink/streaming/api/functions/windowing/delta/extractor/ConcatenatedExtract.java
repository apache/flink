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

/**
 * Combines two extractors which will be executed one after each other.
 *
 * @param <FROM>
 *            The input type of the first extractor.
 * @param <OVER>
 *            The output type of the first and the input type of the second
 *            extractor.
 * @param <TO>
 *            The output type of the second extractor and the output type of the
 *            over all extraction.
 */
@Internal
public class ConcatenatedExtract<FROM, OVER, TO> implements Extractor<FROM, TO> {

	private static final long serialVersionUID = -7807197760725651752L;

	private Extractor<FROM, OVER> e1;
	private Extractor<OVER, TO> e2;

	/**
	 * Combines two extractors which will be executed one after each other.
	 *
	 * @param e1
	 *            First extractor: This extractor gets applied to the input data
	 *            first. Its output as then passed as input to the second
	 *            extractor.
	 * @param e2
	 *            Second extractor: This extractor gets the output of the first
	 *            extractor as input. Its output is then the result of the over
	 *            all extraction.
	 */
	public ConcatenatedExtract(Extractor<FROM, OVER> e1, Extractor<OVER, TO> e2) {
		this.e1 = e1;
		this.e2 = e2;
	}

	@Override
	public TO extract(FROM in) {
		return e2.extract(e1.extract(in));
	}

	public <OUT> ConcatenatedExtract<FROM, TO, OUT> add(Extractor<TO, OUT> e3) {
		return new ConcatenatedExtract<FROM, TO, OUT>(this, e3);
	}

}
