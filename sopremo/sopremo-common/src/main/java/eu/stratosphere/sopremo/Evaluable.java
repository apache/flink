package eu.stratosphere.sopremo;

import org.codehaus.jackson.JsonNode;

public interface Evaluable {

	// public abstract Iterator<JsonNode> evaluateStreams(Iterator<JsonNode>... inputs);
	//
	// public Iterator<JsonNode> evaluateStreams(Iterator<JsonNode> input) {
	// // return new EvaluatingIterator(input);
	// return Arrays.asList(aggregate(input)).iterator();
	// }
	//
	// protected JsonNode aggregate(Iterator<JsonNode> input) {
	// throw new EvaluationException();
	// }
	//
	// protected JsonNode aggregate(Iterator<JsonNode>... inputs) {
	// return aggregate(inputs[0]);
	// }
	//
	public abstract JsonNode evaluate(JsonNode node);
	//
	// public abstract JsonNode evaluate(JsonNode node);

}