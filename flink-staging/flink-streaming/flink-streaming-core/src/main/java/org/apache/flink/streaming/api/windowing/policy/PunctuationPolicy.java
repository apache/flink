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

package org.apache.flink.streaming.api.windowing.policy;

import org.apache.flink.streaming.api.windowing.extractor.Extractor;

/**
 * This policy can be used to trigger and evict based on a punctuation which is
 * present within the arriving data. Using this policy, one can react on an
 * externally defined arbitrary windowing semantic.
 * 
 * In case this policy is used for eviction, the complete buffer will get
 * deleted in case the punctuation is detected.
 * 
 * By default this policy does not react on fake elements. Wrap it in an
 * {@link ActiveEvictionPolicyWrapper} to make it react on punctuation even in
 * fake elements.
 * 
 * @param <IN>
 *            The type of the input data handled by this policy. An
 *            {@link Extractor} can be used to extract DATA for IN.
 * @param <DATA>
 *            The type of the punctuation. An {@link Extractor} can be used to
 *            extract DATA for IN.
 */
public class PunctuationPolicy<IN, DATA> implements CloneableTriggerPolicy<IN>,
		CloneableEvictionPolicy<IN> {

	/**
	 * auto generated version id
	 */
	private static final long serialVersionUID = -8845130188912602498L;
	private int counter = 0;
	private Extractor<IN, DATA> extractor;
	private DATA punctuation;

	/**
	 * Creates the punctuation policy without using any extractor. To make this
	 * work IN and DATA must not be different types.
	 * 
	 * @param punctuation
	 *            the punctuation which leads to trigger/evict.
	 */
	public PunctuationPolicy(DATA punctuation) {
		this(punctuation, null);
	}

	/**
	 * Creates the punctuation policy which uses the specified extractor to
	 * isolate the punctuation from the data.
	 * 
	 * @param punctuation
	 *            the punctuation which leads to trigger/evict.
	 * @param extractor
	 *            An {@link Extractor} which converts IN to DATA.
	 */
	public PunctuationPolicy(DATA punctuation, Extractor<IN, DATA> extractor) {
		this.punctuation = punctuation;
		this.extractor = extractor;
	}

	@Override
	public int notifyEviction(IN datapoint, boolean triggered, int bufferSize) {
		if (notifyTrigger(datapoint)) {
			int tmp = counter;
			// As the current will be add after the eviction the counter needs
			// to be set to one already
			counter = 1;
			return tmp;
		} else {
			counter++;
			return 0;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean notifyTrigger(IN datapoint) {
		DATA tmp;

		// eventually extract data
		if (extractor == null) {
			// unchecked convert (cannot check it here)
			tmp = (DATA) datapoint;
		} else {
			tmp = extractor.extract(datapoint);
		}

		// compare data with punctuation
		if (punctuation.equals(tmp)) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public PunctuationPolicy<IN, DATA> clone() {
		return new PunctuationPolicy<IN, DATA>(punctuation, extractor);
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof PunctuationPolicy)) {
			return false;
		} else {
			try {
				@SuppressWarnings("unchecked")
				PunctuationPolicy<IN, DATA> otherPolicy = (PunctuationPolicy<IN, DATA>) other;
				if (extractor != null) {
					return extractor.getClass() == otherPolicy.extractor.getClass()
							&& punctuation.equals(otherPolicy.punctuation);
				} else {
					return punctuation.equals(otherPolicy.punctuation)
							&& otherPolicy.extractor == null;
				}

			} catch (Exception e) {
				return false;
			}
		}
	}

	@Override
	public String toString() {
		return "PunctuationPolicy(" + punctuation
				+ (extractor != null
					? ", " + extractor.getClass().getSimpleName()
					: "")
				+ ")";
	}
}
