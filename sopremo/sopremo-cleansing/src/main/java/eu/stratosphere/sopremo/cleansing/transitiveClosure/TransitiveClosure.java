package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.record_linkage.ClosureMode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

@InputCardinality(min = 2, max = 2)
public class TransitiveClosure extends CompositeOperator<TransitiveClosure> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7947908032635354614L;

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);
		JsonStream input = sopremoModule.getInput(0);
		JsonStream nullInput = sopremoModule.getInput(1);

		//Preprocessing
		final GenerateMatrix filledMatrix = new GenerateMatrix().withInputs(input, nullInput);
		
		// compute transitive Closure P1
		final Phase1 phase1 = new Phase1().withInputs(filledMatrix);

		// compute transitive Closure P2
		final Phase2 phase2 = new Phase2().withInputs(phase1, filledMatrix);

		// compute transitive Closure P3
		final Phase3 phase3 = new Phase3().withInputs(new UnionAll().withInputs(phase1, phase2));

		// emit Results as Links
		final EmitMatrix result = new EmitMatrix().withInputs(/*new UnionAll().withInputs(phase1, phase2) */phase3);

		sopremoModule.getOutput(0).setInput(0, result);

		return sopremoModule;

	}

	

	public static void warshall(final BinarySparseMatrix matrix) {
		// Warshall
		for (JsonNode row : matrix.getRows()) {
			final Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(matrix.get(row));
			while (!columnsToExplore.isEmpty()) {
				final JsonNode column = columnsToExplore.pop();
				Set<JsonNode> transitiveColumn = matrix.get(column);
				for (final JsonNode transitiveNode : transitiveColumn)
					if (!row.equals(transitiveNode) && !matrix.isSet(row, transitiveNode)) {
						matrix.set(row, transitiveNode);
						columnsToExplore.push(transitiveNode);
					}
			}
		}
		// matrix.makeSymmetric();
	}

	public static void warshall(final BinarySparseMatrix primary, BinarySparseMatrix current) {
		final Deque<JsonNode> rowsToExplore = new LinkedList<JsonNode>(current.getRows());
		while (!rowsToExplore.isEmpty()) {
			JsonNode row = rowsToExplore.pop();
			final Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(primary.get(row));
			while (!columnsToExplore.isEmpty()) {
				final JsonNode column = columnsToExplore.pop();
				for (final JsonNode transitiveNode : current.get(row))
					if (!row.equals(transitiveNode) && !current.isSet(column, transitiveNode)) {
						current.set(column, transitiveNode);
						// columnsToExplore.push(transitiveNode);
					}
			}
		}
	}
	
	public static void warshall(final BinarySparseMatrix primaryRow, final BinarySparseMatrix primaryColumn, BinarySparseMatrix current) {
		final Deque<JsonNode> rowsToExplore = new LinkedList<JsonNode>(primaryRow.getRows());
		while (!rowsToExplore.isEmpty()){
			JsonNode row = rowsToExplore.pop();
			final Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(primaryRow.get(row));
			while (!columnsToExplore.isEmpty()) {
				final JsonNode column = columnsToExplore.pop();
				final Deque<JsonNode> transitiveNodesToExplore = new LinkedList<JsonNode>(primaryColumn.get(column));
				while(!transitiveNodesToExplore.isEmpty()){
					final JsonNode transitiveNode = transitiveNodesToExplore.pop();
					current.set(row, transitiveNode);
				}


			}

		}
	}

	/**
	 * @param closureMode
	 * @return
	 */
	public TransitiveClosure withClosureMode(ClosureMode closureMode) {
		return null;
	}
}
