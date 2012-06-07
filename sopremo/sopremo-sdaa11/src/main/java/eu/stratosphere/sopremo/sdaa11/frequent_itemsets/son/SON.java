/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.sdaa11.RoundRobinAnnotator;

/**
 * @author skruse
 * 
 */
public class SON extends CompositeOperator<SON> {

	private static final long serialVersionUID = -4600530033164134202L;

	public static final int DEFAULT_MIN_SUPPORT = 100;
	public static final int DEFAULT_MAX_SET_SIZE = 1;
	public static final int DEFAULT_PARALLELISM = 1;

	private int parallelism = DEFAULT_PARALLELISM;
	private int minSupport = DEFAULT_MIN_SUPPORT;
	private int maxSetSize = DEFAULT_MAX_SET_SIZE;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 1, 1);

		final Source basketSource = module.getInput(0);
		final RoundRobinAnnotator annotator = new RoundRobinAnnotator()
				.withInputs(basketSource);
		annotator.setMaxAnnotation(this.parallelism - 1);

		final int localMinSupport = this.minSupport / this.parallelism;

		final LocalAPriori aPriori = new LocalAPriori().withInputs(annotator);
		aPriori.setMinSupport(localMinSupport);
		aPriori.setMaxSetSize(this.maxSetSize);

		final MakeCandidatesUnique makeCandidatesUnique = new MakeCandidatesUnique()
				.withInputs(aPriori);

		final MatchFrequentItemsets matchFrequentItemsets = new MatchFrequentItemsets()
				.withInputs(makeCandidatesUnique, basketSource);

		final SelectFrequentItemsets selectFrequentItemsets = new SelectFrequentItemsets()
				.withInputs(matchFrequentItemsets);

		module.getOutput(0).setInput(0, selectFrequentItemsets);

		return module.asElementary();
	}

	/**
	 * Returns the parallelism.
	 * 
	 * @return the parallelism
	 */
	public int getParallelism() {
		return this.parallelism;
	}

	/**
	 * Sets the parallelism to the specified value.
	 * 
	 * @param parallelism
	 *            the parallelism to set
	 */
	public void setParallelism(final int parallelism) {
		this.parallelism = parallelism;
	}

	/**
	 * Returns the minSupport.
	 * 
	 * @return the minSupport
	 */
	public int getMinSupport() {
		return this.minSupport;
	}

	/**
	 * Sets the minSupport to the specified value.
	 * 
	 * @param minSupport
	 *            the minSupport to set
	 */
	public void setMinSupport(final int minSupport) {
		this.minSupport = minSupport;
	}

	/**
	 * Returns the maxSetSize.
	 * 
	 * @return the maxSetSize
	 */
	public int getMaxSetSize() {
		return this.maxSetSize;
	}

	/**
	 * Sets the maxSetSize to the specified value.
	 * 
	 * @param maxSetSize
	 *            the maxSetSize to set
	 */
	public void setMaxSetSize(final int maxSetSize) {
		this.maxSetSize = maxSetSize;
	}

}
