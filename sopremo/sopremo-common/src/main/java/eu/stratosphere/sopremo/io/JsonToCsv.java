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
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;
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

	public void setSeparator(String separator) {
		if (separator == null)
			throw new NullPointerException("separator must not be null");

		this.separator = separator;
	}

	public List<EvaluationExpression> getExtractionExpressions() {
		return this.extractionExpressions;
	}

	public void setContext(EvaluationContext context) {
		if (context == null)
			throw new NullPointerException("context must not be null");

		this.context = context;
	}

	public void setExtractionExpressions(List<EvaluationExpression> extractionExpressions) {
		if (extractionExpressions == null)
			throw new NullPointerException("extractionExpressions must not be null");

		this.extractionExpressions = extractionExpressions;
	}

	public static FileDataSinkContract<Key, PactString> convert(String sourceFile, JsonToCsv jsonToCsv,
			String targetFile) {
		PactModule sourceModule = new Source(sourceFile).asPactModule(jsonToCsv.getContext());
		Contract source = sourceModule.getOutput(0).getInput();

		MapContract<Key, JsonNode, Key, PactString> jsonToString =
			new MapContract<Key, JsonNode, Key, PactString>(JsonToString.class);
		jsonToString.getParameters().setString("separator", jsonToCsv.getSeparator());
		SopremoUtil.setContext(jsonToString.getParameters(), jsonToCsv.getContext());
		SopremoUtil.serialize(jsonToString.getParameters(), "extractionExpressions",
			(Serializable) jsonToCsv.getExtractionExpressions());
		jsonToString.setInput(source);

		FileDataSinkContract<Key, PactString> target = new FileDataSinkContract<Key, PactString>(
			StringOutputFormat.class, targetFile);
		target.setInput(jsonToString);

		return target;
	}

	public static class StringOutputFormat extends TextOutputFormat<Key, PactString> {
		private Charset cs = Charset.defaultCharset();

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.TextOutputFormat#writeLine(eu.stratosphere.pact.common.type.KeyValuePair)
		 */
		@Override
		public byte[] writeLine(KeyValuePair<Key, PactString> pair) {
			ByteBuffer line = this.cs.encode(pair.getValue().getValue());
			byte[] lineArray = new byte[line.limit() + 1];
			line.get(lineArray, 0, line.limit());
			lineArray[line.limit()] = '\n';
			return lineArray;
		}
	}

	public static class JsonToString extends MapStub<Key, JsonNode, Key, PactString> {

		private List<EvaluationExpression> extractionExpressions;

		private EvaluationContext context;

		private String separator;

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.stub.SingleInputStub#configure(eu.stratosphere.nephele.configuration.Configuration
		 * )
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);

			this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
			this.extractionExpressions = (List<EvaluationExpression>) SopremoUtil.deserialize(parameters,
				"extractionExpressions", Serializable.class);
			this.separator = parameters.getString("separator", ";");
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.MapStub#map(eu.stratosphere.pact.common.type.Key,
		 * eu.stratosphere.pact.common.type.Value, eu.stratosphere.pact.common.stub.Collector)
		 */
		@Override
		public void map(Key key, JsonNode value, Collector<Key, PactString> out) {
			StringBuilder string = new StringBuilder();

			value = ((JsonNodeWrapper) value).getValue();
			if (this.extractionExpressions.isEmpty())
				discoverEntries(value, new LinkedList<EvaluationExpression>());
			for (EvaluationExpression expr : this.extractionExpressions)
				string.append(expr.evaluate(value, this.context)).append(this.separator);

			string.setLength(string.length() - 1);

			out.collect(key, new PactString(string.toString()));
		}

		/**
		 * @param value
		 */
		private void discoverEntries(JsonNode value, LinkedList<EvaluationExpression> path) {
			if (value instanceof ObjectNode) {
				for (Entry<String, JsonNode> entry : ((ObjectNode) value).getEntries()) {
					path.push(new ObjectAccess(entry.getKey()));
					discoverEntries(entry.getValue(), path);
					path.pop();
				}
			} else if (value instanceof ArrayNode) {
				for (int index = 0; index < ((ArrayNode) value).size(); index++) {
					path.push(new ArrayAccess(index));
					discoverEntries(((ArrayNode) value).get(index), path);
					path.pop();
				}
			} else
				this.extractionExpressions.add(PathExpression.wrapIfNecessary(path));
		}
	}
}
