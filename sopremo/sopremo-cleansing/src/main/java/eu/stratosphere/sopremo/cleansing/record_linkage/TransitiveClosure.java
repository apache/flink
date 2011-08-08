package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.base.GlobalEnumeration;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoMatch;
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

	private EvaluationExpression idProjection = DEFAULT_PROJECTION;

	private boolean emitClusters = true;

	public TransitiveClosure(final JsonStream input) {
		super(input);
	}

	@Override
	public SopremoModule asElementaryOperators() {
		JsonStream input = this.getInput(0);
		JsonStream backLookup;
		if (this.idProjection == DEFAULT_PROJECTION) {
			final JsonStream entityExtractor = new RemoveDuplicateEntities(new FlattenPairs(input));
			final GlobalEnumeration globalEnumeration = new GlobalEnumeration(entityExtractor);
			globalEnumeration.setIdGeneration(GlobalEnumeration.LONG_COMBINATION);
			input = new SubstituteWithKeyValueList(input, globalEnumeration);
			backLookup = globalEnumeration;
		} else {
			backLookup = new Projection(this.idProjection, EvaluationExpression.SAME_VALUE, input);
			input = new Projection(new ArrayCreation(new PathExpression(new ArrayAccess(0), this.idProjection),
				new PathExpression(new ArrayAccess(1), this.idProjection)), input);
		}

		final Grouping groupAll = new Grouping(EvaluationExpression.SAME_VALUE, input);
		final UnparallelClosure pairs = new UnparallelClosure(this.emitClusters, groupAll);
		return SopremoModule.valueOf(this.getName(), new SubstituteWithKeyValueList(pairs, backLookup));
	}

	public EvaluationExpression getIdProjection() {
		return this.idProjection;
	}

	public boolean isCluster() {
		return this.emitClusters;
	}

	public void setCluster(final boolean cluster) {
		this.emitClusters = cluster;
	}

	public void setIdProjection(final EvaluationExpression idProjection) {
		if (idProjection == null)
			throw new NullPointerException("idProjection must not be null");

		this.idProjection = idProjection;
	}

	public static void warshall(final BinarySparseMatrix matrix) {
		// Warshall
		for (final JsonNode row : matrix.getRows()) {
			final Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(matrix.get(row));
			while (!columnsToExplore.isEmpty()) {
				final JsonNode column = columnsToExplore.pop();
				for (final JsonNode transitiveNode : matrix.get(column))
					if (row != transitiveNode && !matrix.isSet(row, transitiveNode)) {
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
				out.collect(value.get(0), NullNode.getInstance());
				out.collect(value.get(1), NullNode.getInstance());
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
			protected void reduce(final JsonNode key, final StreamArrayNode values, final JsonCollector out) {
				out.collect(NullNode.getInstance(), key);
			}
		}
	}

	public static class SubstituteWithKeyValueList extends CompositeOperator {

		/**
		 * 
		 */
		private static final long serialVersionUID = 5213470669940261166L;

		public SubstituteWithKeyValueList(final JsonStream input, final JsonStream keyValueList) {
			super(input, keyValueList);
		}

		@Override
		public SopremoModule asElementaryOperators() {
			final Projection left = new Projection(new ArrayAccess(0), EvaluationExpression.SAME_VALUE,
				this.getInput(0));
			final ReplaceWithRightInput replacedLeft = new ReplaceWithRightInput(0, left);
			final Projection right = new Projection(new ArrayAccess(1), EvaluationExpression.SAME_VALUE, replacedLeft);
			final ReplaceWithRightInput replacedRight = new ReplaceWithRightInput(1, right);

			return SopremoModule.valueOf(this.getName(), replacedRight);
		}

		public static class ReplaceWithRightInput extends ElementaryOperator {
			/**
			 * 
			 */
			private static final long serialVersionUID = 7334161941683036846L;

			private final int index;

			public ReplaceWithRightInput(final int index, final JsonStream input) {
				super(input);
				this.index = index;
			}

			public int getIndex() {
				return this.index;
			}

			public static class Implementation extends
					SopremoMatch<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
				private int index;

				@Override
				protected void match(final JsonNode key, final JsonNode value1, final JsonNode value2,
						final JsonCollector out) {
					((ArrayNode) value1).set(this.index, value2);
					out.collect(NullNode.getInstance(), value1);
				}
			}
		}

		public static class SwapKeyValue extends ElementaryOperator {
			/**
			 * 
			 */
			private static final long serialVersionUID = 311598721939565997L;

			public SwapKeyValue(final JsonStream input) {
				super(input);
			}

			public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
				@Override
				protected void map(final JsonNode key, final JsonNode value, final JsonCollector out) {
					out.collect(value, key);
				}
			}
		}
	}

	public static class UnparallelClosure extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2445030877785106855L;

		private final boolean emitClusters;

		public UnparallelClosure(final boolean emitClusters, final JsonStream input) {
			super(input);
			this.emitClusters = emitClusters;
		}

		public boolean isEmitClusters() {
			return this.emitClusters;
		}

		public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
			private boolean emitClusters;

			@Override
			protected void map(final JsonNode key, final JsonNode allPairs, final JsonCollector out) {
				final BinarySparseMatrix matrix = new BinarySparseMatrix();
				for (final JsonNode pair : allPairs) {
					matrix.set(pair.get(0), pair.get(1));
					matrix.set(pair.get(1), pair.get(0));
				}

				warshall(matrix);

				if (this.emitClusters) {
					final Set<JsonNode> remainingRows = new HashSet<JsonNode>(matrix.getRows());
					while (!remainingRows.isEmpty()) {
						final ArrayNode cluster = new ArrayNode(null);
						final JsonNode row = remainingRows.iterator().next();
						cluster.add(row);
						for (final JsonNode column : matrix.get(row))
							cluster.add(column);
						remainingRows.remove(cluster);
					}
				} else
					for (final JsonNode row : matrix.getRows())
						for (final JsonNode column : matrix.get(row))
							if (JsonNodeComparator.INSTANCE.compare(row, column) < 0)
								out.collect(key, JsonUtil.asArray(row, column));
			}
		}
	}
}
