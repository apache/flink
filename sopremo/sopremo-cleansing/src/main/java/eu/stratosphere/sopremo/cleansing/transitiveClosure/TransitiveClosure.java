package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

/**
 * Computes the transitive closure within a given undirected graph.
 * The input graph and each edge within it has to be specified as an array in json-notation.
 * The following example shows a small graph with three edges.<br>
 * [ [{ "id": 12},{ "id": 24}], [{ "id": 24},{ "id": 35}], [{ "id": 35},{ "id": 36}] ]<br>
 * Before starting to compute the closure, the graph is converted into a matrix and is devided
 * into a specified number of partitions. For this
 * convertion a second input with at least one node is needed.
 * The whole matrix is now partitioned into several blocks where each block is a matrix himself.
 * After that, the computation of the transitive closure starts.
 */
@InputCardinality(min = 2, max = 2)
public class TransitiveClosure extends CompositeOperator<TransitiveClosure> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7947908032635354614L;

	private int numberOfPartitions = 1;

	/**
	 * Sets the number of partitions. Via default this number is 1.
	 * 
	 * @param number
	 *        the number of partionions that should be used to compute the closure.
	 */
	public void setNumberOfPartitions(int number) {
		this.numberOfPartitions = number;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);
		JsonStream input = sopremoModule.getInput(0);
		JsonStream nullInput = sopremoModule.getInput(1);

		Phase1[] phase1 = new Phase1[this.numberOfPartitions];
		Phase2[] phase2 = new Phase2[this.numberOfPartitions];
		Phase3[] phase3 = new Phase3[this.numberOfPartitions];
		UnionAll itOutput[] = new UnionAll[this.numberOfPartitions];

		// Preprocessing
		final GenerateMatrix filledMatrix = new GenerateMatrix().withInputs(input, nullInput);
		filledMatrix.setNumberOfPartitions(this.numberOfPartitions);

		for (int i = 0; i < this.numberOfPartitions; i++) {
			JsonStream inputStream = i == 0 ? filledMatrix : phase3[i - 1];

			// compute transitive Closure P1
			phase1[i] = new Phase1().withInputs(inputStream);
			phase1[i].setIterationStep(i);

			// compute transitive Closure P2
			phase2[i] = new Phase2().withInputs(phase1[i]);
			phase2[i].setIterationStep(i);

			// compute transitive Closure P3
			phase3[i] = new Phase3().withInputs(phase2[i]);
			phase3[i].setNumberOfPartitions(i);

		}

		// compute transitive Closure P1
		// final Phase1 phase1 = new Phase1().withInputs(filledMatrix);

		// compute transitive Closure P2
		// final Phase2 phase2 = new Phase2().withInputs(phase1, filledMatrix);

		// compute transitive Closure P3
		// final Phase3 phase3 = new Phase3().withInputs(new UnionAll().withInputs(phase1, phase2));
		// phase3.setNumberOfPartitions(this.numberOfPartitions);

		// emit Results as Links
		final EmitMatrix result = new EmitMatrix()
			.withInputs(/* new UnionAll().withInputs(phase1, phase2) */phase3[this.numberOfPartitions - 1]);

		sopremoModule.getOutput(0).setInput(0, result);

		return sopremoModule;

	}

	/**
	 * Computes the transitive closure within the given {@link BinarySparseMatrix}.
	 * 
	 * @param matrix
	 *        the matrix where the transitive closure should be computed for
	 */
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

	/**
	 * Computes the transitive closure for 2 row- or column-aligned sub-{@link BinarySparseMatrix}s. Changes can only be
	 * made to the current-matrix, the primary-matrix is only for lookup.
	 * 
	 * @param primary
	 *        the matrix where edges can be looked up. This matrix is normally a diagonal block of the whole matrix used
	 *        in {@link TransitiveClosure}
	 * @param current
	 *        the matrix where additional edges should be added. This matrix is either row- or column aligned to the
	 *        primary matrix.
	 */
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

	/**
	 * Computes the transitive closure for 3 sub-{@link BinarySparseMatrix}s. Changes can only be made to the
	 * current-matrix, both primary-matrices are only for lookup.
	 * 
	 * @param primaryRow
	 *        the primary-matrix that is row-aligned with the current-matrix
	 * @param primaryColumn
	 *        the primary-matrix that is column-aligned with the current-matrix
	 * @param current
	 *        the matrix where additional edges should be added.
	 */
	public static void warshall(final BinarySparseMatrix primaryRow, final BinarySparseMatrix primaryColumn,
			BinarySparseMatrix current) {
		final Deque<JsonNode> rowsToExplore = new LinkedList<JsonNode>(primaryRow.getRows());
		while (!rowsToExplore.isEmpty()) {
			JsonNode row = rowsToExplore.pop();
			final Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(primaryRow.get(row));
			while (!columnsToExplore.isEmpty()) {
				final JsonNode column = columnsToExplore.pop();
				final Deque<JsonNode> transitiveNodesToExplore = new LinkedList<JsonNode>(primaryColumn.get(column));
				while (!transitiveNodesToExplore.isEmpty()) {
					final JsonNode transitiveNode = transitiveNodesToExplore.pop();
					current.set(row, transitiveNode);
				}
			}
		}
	}
}
