package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import java.util.Deque;
import java.util.LinkedList;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.cleansing.record_linkage.BinarySparseMatrix;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class TransitiveClosure extends CompositeOperator<TransitiveClosure> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7947908032635354614L;

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 1, 1);
		JsonStream input = sopremoModule.getInput(0);

		// TupleDuplicate tuples = new TupleDuplicate().withInputs(input);
		//
		// Grouping group = new Grouping().withInputs(tuples).withGroupingKey(EvaluationExpression.KEY);
		//
		// final PathExpression keyTransform = new PathExpression(new ArrayAccess(0), new ObjectAccess("partition"));
		// final PathExpression valueTransform = new PathExpression(new FunctionCall("add", EvaluationExpression.VALUE,
		// EvaluationExpression.KEY, new ConstantExpression(0)));
		//
		// Projection proj = new Projection().withKeyTransformation(keyTransform)
		// .withValueTransformation(valueTransform).withInputs(group);

		PathExpression transformation = new PathExpression(new ArrayAccess(0), new ObjectAccess("partition"));
		final Projection valueExtraction = new Projection().withInputs(input).withKeyTransformation(transformation);

		final Grouping group = new Grouping().withInputs(valueExtraction).withGroupingKey(EvaluationExpression.KEY);

		final ParallelClosure pairs = new ParallelClosure()
			/* .withClosureMode(this.closureMode) */.withInputs(group);

		sopremoModule.getOutput(0).setInput(0, pairs);
		return sopremoModule;

	}

	public static <E> void warshall(final BinarySparseMatrix<E> matrix) {
		// Warshall
		for (final E row : matrix.getRows()) {
			final Deque<E> columnsToExplore = new LinkedList<E>(matrix.get(row));
			while (!columnsToExplore.isEmpty()) {
				final E column = columnsToExplore.pop();
				for (final E transitiveNode : matrix.get(column))
					if (!row.equals(transitiveNode) && !matrix.isSet(row, transitiveNode)) {
						matrix.set(row, transitiveNode);
						columnsToExplore.push(transitiveNode);
					}
			}
		}
	}

	private static class TupleDuplicate extends ElementaryOperator<TupleDuplicate> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 5940876439025744020L;

		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				key = ((ArrayNode) value).get(0);
				value = ((ArrayNode) value).get(1);

				out.collect(key, value);
				out.collect(value, key);
			}
		}
	}

}
