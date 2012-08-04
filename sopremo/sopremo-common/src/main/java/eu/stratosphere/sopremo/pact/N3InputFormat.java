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
package eu.stratosphere.sopremo.pact;

import static eu.stratosphere.sopremo.pact.IOConstants.ENCODING;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * Simple input format for N3 files which only have lines
 * consistent to the following pattern:<br>
 * <code>&lt;subject&gt;&lt;space&gt;&lt;property&gt;&lt;space&gt;&lt;object&gt;&lt;space&gt;.</code><br>
 * Outputs JSON nodes like this:<br>
 * <code>{"s":&lt;subject&gt;, "p":&lt;property&gt;, "o":&lt;object&gt;}</code>
 * 
 * @author Sebastian Kruse
 */
public class N3InputFormat extends DelimitedInputFormat {

	/**
	 * Read subjects will be placed under this key in the output object.
	 */
	public static final String SUBJECT = "s";

	/**
	 * Read properties will be placed under this key in the output object.
	 */
	public static final String PROPERTY = "p";

	/**
	 * Read objects will be placed under this key in the output object.
	 */
	public static final String OBJECT = "o";

	private EvaluationContext context;

	private Schema schema;

	private final IObjectNode tripleNode = new ObjectNode();

	private final TextNode subjectNode = new TextNode();

	private final TextNode propertyNode = new TextNode();

	private final TextNode objectNode = new TextNode();

	private Charset charset;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.pact.common.io.DelimitedInputFormat#configure(eu.stratosphere.nephele.configuration.Configuration
	 * )
	 */
	@Override
	public void configure(final Configuration parameters) {
		super.configure(parameters);

		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT, EvaluationContext.class);
		this.schema = this.context.getOutputSchema(0);
		if (this.schema == null)
			throw new IllegalStateException("Could not deserialize output schema");
		this.charset = Charset.forName(parameters.getString(ENCODING, "utf-8"));

		this.tripleNode.put(SUBJECT, this.subjectNode);
		this.tripleNode.put(PROPERTY, this.propertyNode);
		this.tripleNode.put(OBJECT, this.objectNode);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord,
	 * byte[], int, int)
	 */
	@Override
	public boolean readRecord(final PactRecord target, final byte[] bytes, final int offset, final int numBytes) {
		final String tripleString = new String(bytes, offset, numBytes, this.charset);
		final int firstSpacePos = tripleString.indexOf(' ');
		final int secondSpacePos = tripleString.indexOf(' ', firstSpacePos + 1);
		final int lastSpacePos = tripleString.lastIndexOf(' ');

		this.subjectNode.setValue(tripleString.substring(0, firstSpacePos));
		this.propertyNode.setValue(tripleString.substring(firstSpacePos + 1, secondSpacePos));
		this.objectNode.setValue(tripleString.substring(secondSpacePos + 1, lastSpacePos));

		final PactRecord result = this.schema.jsonToRecord(this.tripleNode, target, this.context);
		if (result != target)
			result.copyTo(target);

		return true;
	}

}
