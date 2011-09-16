package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoReduce;

/**
 * Expects pairs (array with two elements)
 * 
 * @author Arvid Heise
 */
public class TransitiveClosure extends CompositeOperator {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3678166801854560781L;

	/**
	 * Tag variable
	 */
	private static final EvaluationExpression DEFAULT_PROJECTION = new EvaluationExpression() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1075231292038294405L;

		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			return null;
		}
	};

	// private Int2ObjectArrayMap<EvaluationExpression> idProjections = new Int2ObjectArrayMap<EvaluationExpression>();

	private ClosureMode closureMode = ClosureMode.LINKS;

	public TransitiveClosure(final JsonStream input) {
		super(input);

		// idProjections.defaultReturnValue(DEFAULT_PROJECTION);
	}

	public ClosureMode getClosureMode() {
		return this.closureMode;
	}

	public void setClosureMode(ClosureMode clusterMode) {
		if (clusterMode == null)
			throw new NullPointerException("clusterMode must not be null");

		this.closureMode = clusterMode;
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

		final Grouping groupAll = new Grouping(EvaluationExpression.VALUE, input);
		final UnparallelClosure pairs = new UnparallelClosure(this.closureMode, groupAll);

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

	public static void warshall(final BinarySparseMatrix<Object> matrix) {
		// Warshall
		for (final Object row : matrix.getRows()) {
			final Deque<Object> columnsToExplore = new LinkedList<Object>(matrix.get(row));
			while (!columnsToExplore.isEmpty()) {
				final Object column = columnsToExplore.pop();
				for (final Object transitiveNode : matrix.get(column))
					if (!row.equals(transitiveNode) && !matrix.isSet(row, transitiveNode)) {
						matrix.set(row, transitiveNode);
						columnsToExplore.push(transitiveNode);
					}
			}
		}
	}

	public static class FlattenPairs extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3704755338323086311L;

		public FlattenPairs(final JsonStream input) {
			super(input);
		}

		public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
			@Override
			protected void map(final JsonNode key, final JsonNode value, final JsonCollector out) {
				out.collect(((ArrayNode)value).get(0), NullNode.getInstance());
				out.collect(((ArrayNode)value).get(1), NullNode.getInstance());
			}
		}
	}

	public static class RemoveDuplicateEntities extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4398233623518806826L;

		public RemoveDuplicateEntities(final JsonStream input) {
			super(input);
		}

		public static class Implementation extends SopremoReduce<Key, PactJsonObject, Key, PactJsonObject> {
			@Override
			protected void reduce(final JsonNode key, final ArrayNode values, final JsonCollector out) {
				out.collect(NullNode.getInstance(), key);
			}
		}
	}

	public static class UnparallelClosure extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2445030877785106855L;

		private final ClosureMode closureMode;

		public UnparallelClosure(final ClosureMode closureMode, final JsonStream input) {
			super(input);
			this.closureMode = closureMode;
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

		public static class Link extends ImplementationBase<Key, PactJsonObject, Key, PactJsonObject> {

			@Override
			protected void emit(final JsonNode key, BinarySparseMatrix<?> genMatrix, final JsonCollector out) {
				BinarySparseMatrix<JsonNode> matrix = (BinarySparseMatrix<JsonNode>) genMatrix;
				for (final JsonNode row : matrix.getRows())
					for (final JsonNode column : matrix.get(row))
						if (JsonNodeComparator.INSTANCE.compare(row, column) < 0)
							out.collect(key, JsonUtil.asArray(row, column));
			}
		}

		public static class Cluster extends ImplementationBase<Key, PactJsonObject, Key, PactJsonObject> {

			@Override
			protected void emit(final JsonNode key, BinarySparseMatrix genMatrix, final JsonCollector out) {
				BinarySparseMatrix<JsonNode> matrix = genMatrix;

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

		public static class Provenance extends ImplementationBase<Key, PactJsonObject, Key, PactJsonObject> {
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
			protected void emit(final JsonNode key, BinarySparseMatrix genMatrix, final JsonCollector out) {
				BinarySparseMatrix<ProvenancedItem<JsonNode>> matrix = genMatrix;

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

		public abstract static class ImplementationBase<IK extends PactJsonObject.Key, IV extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
				extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
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
			ProvenancedItem other = (ProvenancedItem) obj;
			return this.sourceIndex == other.sourceIndex && this.node.equals(other.node);
		}

		@Override
		public String toString() {
			return String.format("ProvenancedItem [node=%s, sourceIndex=%s]", this.node, this.sourceIndex);
		}

	}
}
