package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.cleansing.conflict_resolution.FilterRecordResolution;
import eu.stratosphere.sopremo.cleansing.conflict_resolution.UnresolvableEvalatuationException;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class Scrub extends ElementaryOperator {
	private EvaluationExpression projection;

	public Scrub(EvaluationExpression projection, JsonStream input) {
		super(input);
		this.projection = projection;
	}

	public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
		private EvaluationExpression projection;

		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			try {
				out.collect(key, projection.evaluate(value, getContext()));
			} catch (UnresolvableEvalatuationException e) {
				// do not emit invalid record
			}
		}
	}
}
