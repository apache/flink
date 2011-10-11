package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import java.util.Deque;
import java.util.LinkedList;

import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.cleansing.record_linkage.BinarySparseMatrix;
import eu.stratosphere.sopremo.cleansing.record_linkage.ClosureMode;

import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class ParallelClosure extends ElementaryOperator<ParallelClosure> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6221666263918022600L;

	private ClosureMode closureMode = ClosureMode.LINKS;

	public void setClosureMode(ClosureMode closureMode) {
		if (closureMode == null)
			throw new NullPointerException("closureMode must not be null");

		this.closureMode = closureMode;
	}

	public ParallelClosure withClosureMode(ClosureMode mode) {
		this.setClosureMode(mode);
		return this;
	}

	public ClosureMode getClosureMode() {
		return this.closureMode;
	}

	@Override
	protected Class<? extends Stub<?, ?>> getStubClass() {
		switch (this.closureMode) {
		case LINKS:
			return Link.class;
//		case CLUSTER:
//			return Cluster.class;
//		case CLUSTER_PROVENANCE:
//			return Provenance.class;

		default:
			throw new UnsupportedOperationException();
		}
	}

	public abstract static class ImplementationBase<IK extends JsonNode, IV extends JsonNode, OK extends JsonNode, OV extends JsonNode>
			extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
		private BinarySparseMatrix<Object> matrix = new BinarySparseMatrix<Object>();

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
		
		@Override
		protected void map(final JsonNode key, final JsonNode pairs, final JsonCollector out) {
			this.fillMatrix(this.matrix, pairs);

			warshall(this.matrix);

			this.emit(key, this.matrix, out);

			this.matrix = new BinarySparseMatrix<Object>();
		}

		protected abstract void emit(final JsonNode key, BinarySparseMatrix<?> matrix, final JsonCollector out);

		protected void fillMatrix(BinarySparseMatrix<?> genMatrix, final JsonNode pairs) {
			@SuppressWarnings("unchecked")
			BinarySparseMatrix<JsonNode> matrix = (BinarySparseMatrix<JsonNode>) genMatrix;

			for (final JsonNode pair : (ArrayNode) pairs) {
				JsonNode value1 = null, value2 = null;
				for (int sourceIndex = 0; sourceIndex < ((ArrayNode) pair).size(); sourceIndex++) {
					JsonNode value = ((ArrayNode) pair).get(sourceIndex);
					if (value != NullNode.getInstance())
						if (value1 == null)
							value1 = value;
						else {
							value2 = value;
							break;
						}
				}
				matrix.set(value1, value2);
				matrix.set(value2, value1);
			}
		}
	}
	
	public static class Link extends ImplementationBase<JsonNode, JsonNode, JsonNode, JsonNode> {

		@Override
		protected void emit(final JsonNode key, BinarySparseMatrix<?> genMatrix, final JsonCollector out) {
			@SuppressWarnings("unchecked")
			BinarySparseMatrix<JsonNode> matrix = (BinarySparseMatrix<JsonNode>) genMatrix;
			for (final JsonNode row : matrix.getRows())
				for (final JsonNode column : matrix.get(row))
					if (JsonNodeComparator.INSTANCE.compare(row, column) < 0)
						out.collect(key, JsonUtil.asArray(row, column));
		}
	}
}
