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
package eu.stratosphere.sopremo.io;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author Arvid Heise
 */
public class JsonToCsv {
	/**
	 * 
	 */
	private static final List<EvaluationExpression> AUTO = new ArrayList<EvaluationExpression>();

	private List<EvaluationExpression> extractionExpressions = AUTO;

	private EvaluationContext context = new EvaluationContext();

	private String separator = ";";

	public EvaluationContext getContext() {
		return this.context;
	}

	public String getSeparator() {
		return this.separator;
	}

	public void setSeparator(final String separator) {
		if (separator == null)
			throw new NullPointerException("separator must not be null");

		this.separator = separator;
	}

	public List<EvaluationExpression> getExtractionExpressions() {
		return this.extractionExpressions;
	}

	public void setContext(final EvaluationContext context) {
		if (context == null)
			throw new NullPointerException("context must not be null");

		this.context = context;
	}

	public void setExtractionExpressions(final List<EvaluationExpression> extractionExpressions) {
		if (extractionExpressions == null)
			throw new NullPointerException("extractionExpressions must not be null");

		this.extractionExpressions = extractionExpressions;
	}

	public static FileDataSink convert(final String sourceFile, final JsonToCsv jsonToCsv,
			final String targetFile) {
		final PactModule sourceModule = new Source(sourceFile).asPactModule(jsonToCsv.getContext());
		final Contract source = sourceModule.getOutput(0).getInputs().get(0);

		final MapContract jsonToString = new MapContract(JsonToString.class);
		jsonToString.getParameters().setString("separator", jsonToCsv.getSeparator());
		SopremoUtil.serialize(jsonToString.getParameters(), SopremoUtil.CONTEXT, jsonToCsv.getContext());
		SopremoUtil.serialize(jsonToString.getParameters(), "extractionExpressions",
			(Serializable) jsonToCsv.getExtractionExpressions());
		jsonToString.setInput(source);

		final FileDataSink target = new FileDataSink(StringOutputFormat.class, targetFile);
		target.setInput(jsonToString);

		return target;
	}

	public static class StringOutputFormat extends DelimitedOutputFormat {
		private final Charset cs = Charset.defaultCharset();

		private final PactString string = new PactString();

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.io.DelimitedOutputFormat#serializeRecord(eu.stratosphere.pact.common.type.PactRecord
		 * , byte[])
		 */
		@Override
		public int serializeRecord(final PactRecord record, final byte[] target) throws Exception {
			record.getFieldInto(0, this.string);
			final ByteBuffer line = this.cs.encode(this.string.getValue());

			final int length = line.limit() + 1;
			if (length > target.length)
				return -length;

			line.get(target);
			target[length - 1] = '\n';
			return length;
		}
	}

	public static class JsonToString extends MapStub {

		private List<EvaluationExpression> extractionExpressions;

		private EvaluationContext context;

		private String separator;

		private IJsonNode node;

		private final PactString resultString = new PactString();

		private final PactRecord resultRecord = new PactRecord(this.resultString);

		private Schema schema;

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void open(final Configuration parameters) throws Exception {
			super.open(parameters);

			this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
			this.extractionExpressions = (List<EvaluationExpression>) SopremoUtil.deserialize(parameters,
				"extractionExpressions", Serializable.class);
			this.schema = this.context.getInputSchema(0);
			this.separator = parameters.getString("separator", ";");
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(final PactRecord record, final Collector<PactRecord> out) throws Exception {
			final StringBuilder string = new StringBuilder();

			this.node = this.schema.recordToJson(record, this.node);
			if (this.extractionExpressions.isEmpty())
				this.discoverEntries(this.node, new LinkedList<EvaluationExpression>());
			for (final EvaluationExpression expr : this.extractionExpressions)
				string.append(expr.evaluate(this.node, null, this.context)).append(this.separator);

			string.setLength(string.length() - 1);
			this.resultString.setValue(string.toString());

			out.collect(this.resultRecord);
		}

		/**
		 * @param value
		 */
		private void discoverEntries(final IJsonNode value, final LinkedList<EvaluationExpression> path) {
			if (value instanceof ObjectNode)
				for (final Entry<String, IJsonNode> entry : (IObjectNode) value) {
					path.push(new ObjectAccess(entry.getKey()));
					this.discoverEntries(entry.getValue(), path);
					path.pop();
				}
			else if (value instanceof ArrayNode)
				for (int index = 0; index < ((IArrayNode) value).size(); index++) {
					path.push(new ArrayAccess(index));
					this.discoverEntries(((IArrayNode) value).get(index), path);
					path.pop();
				}
			else
				this.extractionExpressions.add(PathExpression.wrapIfNecessary(path));
		}
	}
}
