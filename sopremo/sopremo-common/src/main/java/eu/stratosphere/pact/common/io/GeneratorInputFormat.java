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
import java.util.Arrays;
import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.IOConstants;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * Input format that reads values from the config and outputs them.
 * 
 * @author skruse
 * @author Arvid Heise
 */
public class GeneratorInputFormat extends GenericInputFormat {

	/**
	 * 
	 */
	private EvaluationContext context;

	/**
	 * Config key which describes the adhoc expression.
	 */
	public static final String ADHOC_EXPRESSION_PARAMETER_KEY = "sopremo.source.generator.expression";

	/**
	 * Iterates over all values.
	 */
	private Iterator<IJsonNode> valueIterator;

	private int numValues = 1;

	/**
	 * Indices that describe the currently opened input split.
	 */
	private int start;

	/**
	 * Schema loaded from config.
	 */
	private Schema schema;

	@Override
	public void configure(final Configuration parameters) {
		super.configure(parameters);

		this.schema = SopremoUtil.deserialize(parameters, IOConstants.SCHEMA, Schema.class);
		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT, EvaluationContext.class);
		final EvaluationExpression expression =
			SopremoUtil.deserialize(parameters, ADHOC_EXPRESSION_PARAMETER_KEY, EvaluationExpression.class);
		IJsonNode value = expression.evaluate(NullNode.getInstance(), null, this.context);

		if (value.isArray()) {
			numValues = ((ArrayNode) value).size();
			this.valueIterator = ((IArrayNode) value).iterator();
		}
		else
			this.valueIterator = Arrays.asList(value).iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.common.io.GenericInputFormat#createInputSplits(int)
	 */
	@Override
	public GeneratorInputSplit[] createInputSplits(final int minNumSplits)
			throws IOException {
		final int numInputSplits = Math.min(minNumSplits, this.numValues);
		final GeneratorInputSplit[] inputSplits = new GeneratorInputSplit[numInputSplits];

		int start = 0;
		int end;
		for (int i = 0; i < numInputSplits; i++) {
			end = (i + 1) * this.numValues / numInputSplits;
			inputSplits[i] = new GeneratorInputSplit(i, start, end);
			start = end;
		}

		return inputSplits;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.common.io.GenericInputFormat#getInputSplitType()
	 */
	@Override
	public Class<GeneratorInputSplit> getInputSplitType() {
		return GeneratorInputSplit.class;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.common.io.GenericInputFormat#open(eu.stratosphere
	 * .nephele.template.GenericInputSplit)
	 */
	@Override
	public void open(final GenericInputSplit split) throws IOException {
		super.open(split);

		if (split == null || !(split instanceof GeneratorInputSplit))
			throw new IOException("Invalid InputSplit: " + split);

		// copy the values from the input split in case that object will be
		// reused
		final GeneratorInputSplit generatorSplit = (GeneratorInputSplit) split;
		this.start = generatorSplit.start;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !this.valueIterator.hasNext();
	}

	@Override
	public boolean nextRecord(final PactRecord record) throws IOException {
		if (this.reachedEnd())
			throw new IOException("End of input split is reached");

		final IJsonNode value = this.valueIterator.next();
		PactRecord result = this.schema.jsonToRecord(value, record, this.context);
		if(result != record)
			result.copyTo(record);
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
