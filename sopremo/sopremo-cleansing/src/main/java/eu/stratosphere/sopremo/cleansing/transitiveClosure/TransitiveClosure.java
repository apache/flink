package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import java.util.Deque;
import java.util.LinkedList;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.cleansing.record_linkage.BinarySparseMatrix;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;

public class TransitiveClosure extends CompositeOperator<TransitiveClosure> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7947908032635354614L;

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 1, 1);
		JsonStream input = sopremoModule.getInput(0);

		PathExpression transformation = new PathExpression(new ArrayAccess(0), new ObjectAccess("partition"));
		final Projection valueExtraction = new Projection().withInputs(input).withKeyTransformation(transformation);
		
		final Grouping groupAll = new Grouping().withInputs(valueExtraction).withGroupingKey(EvaluationExpression.KEY);

		final ParallelClosure pairs = new ParallelClosure()
			/* .withClosureMode(this.closureMode) */.withInputs(groupAll);

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

}
