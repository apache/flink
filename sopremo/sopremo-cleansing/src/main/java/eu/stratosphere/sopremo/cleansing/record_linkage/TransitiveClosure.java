package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoReduce;

/**
 * Expects pairs (array with two elements)
 * 
 * @author Arvid Heise
 */
public class TransitiveClosure extends CompositeOperator<TransitiveClosure> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3678166801854560781L;

	//
	// /**
	// * Tag variable
	// */
	// private static final EvaluationExpression DEFAULT_PROJECTION = new EvaluationExpression() {
	// /**
	// *
	// */
	// private static final long serialVersionUID = 1075231292038294405L;
	//
	// @Override
	// public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
	// return null;
	// }
	// };
	//
	public TransitiveClosure() {
	}

	private ClosureMode closureMode = ClosureMode.LINKS;

	public ClosureMode getClosureMode() {
		return this.closureMode;
	}

	public void setClosureMode(ClosureMode clusterMode) {
		if (clusterMode == null)
			throw new NullPointerException("clusterMode must not be null");

		this.closureMode = clusterMode;
	}

	public TransitiveClosure withClosureMode(ClosureMode closureMode) {
		this.setClosureMode(closureMode);
		return this;
	}

	@Override
	public SopremoModule asElementaryOperators() {

		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 1, 1);
		JsonStream input = sopremoModule.getInput(0);

		// switch (closureMode) {
		// case LINKS:
		//
		// break;
		//
		// default:
		// break;
		// }
		// Int2ObjectArrayMap<Operator> backLookup = new Int2ObjectArrayMap<Operator>();
		// for(int index = 0; index < idProjections.)
		//
		// if (this.idProjection == DEFAULT_PROJECTION) {
		// // final JsonStream entityExtractor = new RemoveDuplicateEntities(new FlattenPairs(input));
		// final JsonStream entityExtractor = new RemoveDuplicateEntities(new ValueSplitter(input)
		// .withArrayProjection(EvaluationExpression.VALUE).withKeyProjection(new ArrayAccess(0))
		// .withValueProjection(EvaluationExpression.NULL));
		// final GlobalEnumeration globalEnumeration = new GlobalEnumeration(entityExtractor);
		// globalEnumeration.setIdGeneration(GlobalEnumeration.LONG_COMBINATION);
		//
		// final Operator element2Id = new Projection(EvaluationExpression.VALUE, EvaluationExpression.KEY,
		// globalEnumeration);
		//
		// input = new Lookup(input, element2Id).withInputKeyExtractor(new ArrayAccess(0));
		// input = new Lookup(input, element2Id).withInputKeyExtractor(new ArrayAccess(1));
		// backLookup1 = backLookup2 = globalEnumeration;
		// } else {
		// final Operator values = new ValueSplitter(input).withArrayProjection(
		// EvaluationExpression.VALUE).withKeyProjection(new PathExpression(new ArrayAccess(0), this.idProjection));
		// backLookup1 = backLookup2 = new Grouping(new ArrayAccess(0), values).withKeyProjection(
		// EvaluationExpression.KEY).withResetKey(false);
		// if (this.closureMode.isCluster()) {
		// // throw new UnsupportedOperationException();
		// // backLookup1 = new Grouping(new ArrayAccess(0), input).withResetKey(false)
		// // .withKeyProjection(new PathExpression(new ArrayAccess(0), this.idProjection))
		// // .withValueProjection(new ArrayAccess(0));
		// // backLookup2 = new Grouping(new ArrayAccess(0), input).withResetKey(false)
		// // .withKeyProjection(new PathExpression(new ArrayAccess(1), this.idProjection))
		// // .withValueProjection(new ArrayAccess(1));
		// } else {
		// // final Projection idExtraction = new Projection(this.idProjection, EvaluationExpression.VALUE,
		// // valueSplitter);
		// // backLookup1 = backLookup2 = new Grouping(new ArrayAccess(0), idExtraction).withKeyProjection(
		// // EvaluationExpression.KEY).withResetKey(false);
		// }
		// input = new Projection(new ArrayCreation(new PathExpression(new ArrayAccess(0), this.idProjection),
		// new PathExpression(new ArrayAccess(1), this.idProjection)), input);
		// }

		final Grouping groupAll = new Grouping().withInputs(input);
		final UnparallelClosure pairs = new UnparallelClosure().withClosureMode(this.closureMode).withInputs(groupAll);

		// Operator output;
		// if (this.closureMode.isCluster())
		// output = new Lookup(pairs, backLookup1).withArrayElementsReplacement(true)
		// .withDictionaryKeyExtraction(EvaluationExpression.KEY);
		// else {
		// final Lookup lookupLeft = new Lookup(pairs, backLookup1).withInputKeyExtractor(new ArrayAccess(0))
		// .withDictionaryKeyExtraction(EvaluationExpression.KEY);
		// output = new Lookup(lookupLeft, backLookup2).withInputKeyExtractor(new ArrayAccess(1))
		// .withDictionaryKeyExtraction(EvaluationExpression.KEY);
		// }

		sopremoModule.getOutput(0).setInput(0, pairs);
		return sopremoModule;
	}

	// public EvaluationExpression getIdProjection(int index) {
	// return this.idProjections.get(index);
	// }
	//
	// public void setIdProjection(int index, final EvaluationExpression idProjection) {
	// if (idProjection == null)
	// throw new NullPointerException("idProjection must not be null");
	//
	// this.idProjections.put (index, idProjection);
	// }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.closureMode.hashCode();
		// result = prime * result + this.idProjections.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		TransitiveClosure other = (TransitiveClosure) obj;
		return this.closureMode == other.closureMode; // &&this.idProjections.equals(other.idProjections)
	}

	@Override
	public String toString() {
		return "TransitiveClosure [closureMode=" + this.closureMode + "]";
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

	public static class FlattenPairs extends ElementaryOperator<FlattenPairs> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3704755338323086311L;

		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
			@Override
			protected void map(final JsonNode key, final JsonNode value, final JsonCollector out) {
				out.collect(((ArrayNode)value).get(0), NullNode.getInstance());
				out.collect(((ArrayNode)value).get(1), NullNode.getInstance());
			}
		}
	}

	public static class RemoveDuplicateEntities extends ElementaryOperator<RemoveDuplicateEntities> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4398233623518806826L;

		public static class Implementation extends SopremoReduce<JsonNode, JsonNode, JsonNode, JsonNode> {
			@Override
			protected void reduce(final JsonNode key, final ArrayNode values, final JsonCollector out) {
				out.collect(NullNode.getInstance(), key);
			}
		}
	}

	public static class UnparallelClosure extends ElementaryOperator<UnparallelClosure> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2445030877785106855L;

		private ClosureMode closureMode = ClosureMode.LINKS;

		public void setClosureMode(ClosureMode closureMode) {
			if (closureMode == null)
				throw new NullPointerException("closureMode must not be null");

			this.closureMode = closureMode;
		}

		public UnparallelClosure withClosureMode(ClosureMode mode) {
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
			case CLUSTER:
				return Cluster.class;
			case CLUSTER_PROVENANCE:
				return Provenance.class;

			default:
				throw new UnsupportedOperationException();
			}
		}

		public static class Link extends ImplementationBase<JsonNode, JsonNode, JsonNode, JsonNode> {

			@Override
			protected void emit(final JsonNode key, BinarySparseMatrix<?> genMatrix, final JsonCollector out) {
				@SuppressWarnings("unchecked")
				BinarySparseMatrix<JsonNode> matrix = (BinarySparseMatrix<JsonNode>) genMatrix;
				for (final JsonNode row : matrix.getRows())
					for (final JsonNode column : matrix.get(row))
						if (row.compareTo(column) < 0)
							out.collect(key, JsonUtil.asArray(row, column));
			}
		}

		public static class Cluster extends ImplementationBase<JsonNode, JsonNode, JsonNode, JsonNode> {

			@Override
			protected void emit(final JsonNode key, BinarySparseMatrix<?> genMatrix, final JsonCollector out) {
				@SuppressWarnings("unchecked")
				BinarySparseMatrix<JsonNode> matrix = (BinarySparseMatrix<JsonNode>) genMatrix;

				final Set<JsonNode> remainingRows = new HashSet<JsonNode>(matrix.getRows());
				while (!remainingRows.isEmpty()) {
					final ArrayNode cluster = new ArrayNode();
					final JsonNode row = remainingRows.iterator().next();
					cluster.add(row);
					for (final JsonNode column : matrix.get(row))
						cluster.add(column);
					for (JsonNode element : cluster)
						remainingRows.remove(element);
					out.collect(key, cluster);
				}
			}
		}

		public static class Provenance extends ImplementationBase<JsonNode, JsonNode, JsonNode, JsonNode> {
			private transient int sourceCount;

			private JsonNode toProvenanceCluster(ProvenancedItem<JsonNode> row,
					Collection<ProvenancedItem<JsonNode>> cluster) {
				final ArrayNode[] provenanceCluster = new ArrayNode[this.sourceCount];
				for (int index = 0; index < provenanceCluster.length; index++)
					provenanceCluster[index] = new ArrayNode();
				provenanceCluster[row.getSourceIndex()].add(row.getNode());
				for (ProvenancedItem<JsonNode> node : cluster)
					provenanceCluster[node.getSourceIndex()].add(node.getNode());
				return JsonUtil.asArray(provenanceCluster);
			}

			@Override
			protected void emit(final JsonNode key, BinarySparseMatrix<?> genMatrix, final JsonCollector out) {
				@SuppressWarnings("unchecked")
				BinarySparseMatrix<ProvenancedItem<JsonNode>> matrix = (BinarySparseMatrix<ProvenancedItem<JsonNode>>) genMatrix;

				final Set<ProvenancedItem<JsonNode>> remainingRows = new HashSet<ProvenancedItem<JsonNode>>(
					matrix.getRows());
				while (!remainingRows.isEmpty()) {
					final ProvenancedItem<JsonNode> row = remainingRows.iterator().next();

					remainingRows.remove(row);
					Set<ProvenancedItem<JsonNode>> cluster = matrix.get(row);
					for (final ProvenancedItem<JsonNode> column : cluster)
						remainingRows.remove(column);
					out.collect(key, this.toProvenanceCluster(row, cluster));
				}
			}

			@Override
			protected void fillMatrix(BinarySparseMatrix<?> genMatrix, final JsonNode pairs) {
				@SuppressWarnings("unchecked")
				BinarySparseMatrix<ProvenancedItem<JsonNode>> matrix = (BinarySparseMatrix<ProvenancedItem<JsonNode>>) genMatrix;

				for (final JsonNode pair : (ArrayNode)pairs) {
					ProvenancedItem<JsonNode> value1 = null, value2 = null;
					for (int sourceIndex = 0; sourceIndex < ((ArrayNode)pair).size(); sourceIndex++) {
						JsonNode value = ((ArrayNode)pair).get(sourceIndex);
						if (value != NullNode.getInstance())
							if (value1 == null)
								value1 = new ProvenancedItem<JsonNode>(value, sourceIndex);
							else {
								value2 = new ProvenancedItem<JsonNode>(value, sourceIndex);
								break;
							}
					}
					matrix.set(value1, value2);
					matrix.set(value2, value1);
				}

				this.sourceCount = ((ArrayNode)((ArrayNode)pairs).get(((ArrayNode)pairs).size() - 1)).size();
			}
		}

		public abstract static class ImplementationBase<IK extends JsonNode, IV extends JsonNode, OK extends JsonNode, OV extends JsonNode>
				extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
			private BinarySparseMatrix<Object> matrix = new BinarySparseMatrix<Object>();

			@Override
			protected void map(final JsonNode key, final JsonNode pairs, final JsonCollector out) {
				this.fillMatrix(this.matrix, pairs);

				warshall(this.matrix);

				this.emit(key, this.matrix, out);

				this.matrix = null;
			}

			protected abstract void emit(final JsonNode key, BinarySparseMatrix<?> matrix, final JsonCollector out);

			protected void fillMatrix(BinarySparseMatrix<?> genMatrix, final JsonNode pairs) {
				@SuppressWarnings("unchecked")
				BinarySparseMatrix<JsonNode> matrix = (BinarySparseMatrix<JsonNode>) genMatrix;

				for (final JsonNode pair : (ArrayNode)pairs) {
					JsonNode value1 = null, value2 = null;
					for (int sourceIndex = 0; sourceIndex < ((ArrayNode)pair).size(); sourceIndex++) {
						JsonNode value = ((ArrayNode)pair).get(sourceIndex);
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
	}

	private static class ProvenancedItem<T> {
		private final T node;

		private final int sourceIndex;

		public ProvenancedItem(T node, int sourceIndex) {
			this.node = node;
			this.sourceIndex = sourceIndex;
		}

		public T getNode() {
			return this.node;
		}

		public int getSourceIndex() {
			return this.sourceIndex;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.node.hashCode();
			result = prime * result + this.sourceIndex;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			ProvenancedItem<?> other = (ProvenancedItem<?>) obj;
			return this.sourceIndex == other.sourceIndex && this.node.equals(other.node);
		}

		@Override
		public String toString() {
			return String.format("ProvenancedItem [node=%s, sourceIndex=%s]", this.node, this.sourceIndex);
		}

	}
}
