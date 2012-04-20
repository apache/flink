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

package eu.stratosphere.pact.common.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Input format that reads values from the config and outputs them.
 * 
 * @author skruse
 */
public class GeneratorInputFormat extends GenericInputFormat {

	/**
	 * Config key which describes a separated list of values.
	 */
	public static final String VALUE_LIST_PARAMETER_KEY = "pact.input.generator.values";

	/**
	 * Config key which describes the delimiter for {@link #VALUE_LIST_PARAMETER_KEY}.
	 * Defaults to {@value #DEFAULT_VALUE_LIST_DELIMITER}.
	 */
	public static final String VALUE_LIST_DELIMITER_PARAMETER_KEY = "pact.input.generator.delimiter";

	/**
	 * The default delimiter for the value list.
	 */
	public static final String DEFAULT_VALUE_LIST_DELIMITER = ",";

	/**
	 * Contains the values that are loaded from the configuration's value list.
	 */
	protected List<String> values;

	/**
	 * Indices that describe the currently opened input split.
	 */
	private int index, start, end;

	/**
	 * Reusable for output.
	 */
	private final PactString pactString = new PactString();

	@Override
	public void configure(final Configuration parameters) {
		super.configure(parameters);

		final String valueList = parameters.getString(VALUE_LIST_PARAMETER_KEY, "");
		final String delimiter = parameters.getString(VALUE_LIST_DELIMITER_PARAMETER_KEY, DEFAULT_VALUE_LIST_DELIMITER);
		final StringTokenizer tokenizer = new StringTokenizer(valueList, delimiter);
		this.values = new ArrayList<String>(tokenizer.countTokens());
		while (tokenizer.hasMoreTokens())
			this.values.add(tokenizer.nextToken());

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.GenericInputFormat#createInputSplits(int)
	 */
	@Override
	public GeneratorInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
		final int numInputSplits = Math.min(minNumSplits, this.values.size());
		final GeneratorInputSplit[] inputSplits = new GeneratorInputSplit[numInputSplits];

		int start = 0;
		int end;
		for (int i = 0; i < numInputSplits; i++) {
			end = (i + 1) * this.values.size() / numInputSplits;
			inputSplits[i] = new GeneratorInputSplit(i, start, end);
			start = end;
		}

		return inputSplits;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.GenericInputFormat#getInputSplitType()
	 */
	@Override
	public Class<GeneratorInputSplit> getInputSplitType() {
		return GeneratorInputSplit.class;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.GenericInputFormat#open(eu.stratosphere.nephele.template.GenericInputSplit)
	 */
	@Override
	public void open(final GenericInputSplit split) throws IOException {
		super.open(split);

		if (split == null || !(split instanceof GeneratorInputSplit))
			throw new IOException("Invalid InputSplit: " + split);

		// copy the values from the input split in case that object will be reused
		final GeneratorInputSplit generatorSplit = (GeneratorInputSplit) split;
		this.start = generatorSplit.start;
		this.index = this.start;
		this.end = generatorSplit.end;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.index >= this.end;
	}

	@Override
	public boolean nextRecord(final PactRecord record) throws IOException {
		if (this.reachedEnd())
			throw new IOException("End of input split is reached");

		this.pactString.setValue(this.values.get(this.index++));
		record.setField(0, this.pactString);

		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#close()
	 */
	@Override
	public void close() throws IOException {
		// nothing to do here
	}

}
