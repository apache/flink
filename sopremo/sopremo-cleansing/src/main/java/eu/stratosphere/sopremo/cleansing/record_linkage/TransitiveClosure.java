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
	 * Tag variable
	 */
	private static final EvaluationExpression DEFAULT_PROJECTION = new EvaluationExpression() {
		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			return null;
		}
	};

	private EvaluationExpression idProjection = DEFAULT_PROJECTION;

	private boolean emitClusters = true;

	public TransitiveClosure(JsonStream input) {
		super(input);
	}

	public EvaluationExpression getIdProjection() {
		return idProjection;
	}

	public boolean isCluster() {
		return emitClusters;
	}

	public void setCluster(boolean cluster) {
		this.emitClusters = cluster;
	}

	public void setIdProjection(EvaluationExpression idProjection) {
		if (idProjection == null)
			throw new NullPointerException("idProjection must not be null");

		this.idProjection = idProjection;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		JsonStream input = getInput(0);
		JsonStream backLookup;
		if (idProjection == DEFAULT_PROJECTION) {
			JsonStream entityExtractor = new RemoveDuplicateEntities(new FlattenPairs(input));
			GlobalEnumeration globalEnumeration = new GlobalEnumeration(entityExtractor);
			globalEnumeration.setIdGeneration(GlobalEnumeration.LONG_COMBINATION);
			input = new SubstituteWithKeyValueList(input, globalEnumeration);
			backLookup = globalEnumeration;
		} else {
			backLookup = new Projection(idProjection, EvaluationExpression.SAME_VALUE, input);
			input = new Projection(new ArrayCreation(new PathExpression(new ArrayAccess(0), idProjection),
				new PathExpression(new ArrayAccess(1), idProjection)), input);
		}

		Grouping groupAll = new Grouping(EvaluationExpression.SAME_VALUE, input);
		UnparallelClosure pairs = new UnparallelClosure(emitClusters, groupAll);
		return SopremoModule.valueOf(getName(), new SubstituteWithKeyValueList(pairs, backLookup));
	}

	public static void warshall(BinarySparseMatrix matrix) {
		// Warshall
		for (JsonNode row : matrix.getRows()) {
			Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(matrix.get(row));
			while (!columnsToExplore.isEmpty()) {
				JsonNode column = columnsToExplore.pop();
				for (JsonNode transitiveNode : matrix.get(column))
					if (row != transitiveNode && !matrix.isSet(row, transitiveNode)) {
						matrix.set(row, transitiveNode);
						columnsToExplore.push(transitiveNode);
					}
			}
		}
	}

	public static class UnparallelClosure extends ElementaryOperator {
		private boolean emitClusters;

		public UnparallelClosure(boolean emitClusters, JsonStream input) {
			super(input);
			this.emitClusters = emitClusters;
		}

		public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
			private boolean emitClusters;

			@Override
			protected void map(JsonNode key, JsonNode allPairs, JsonCollector out) {
				BinarySparseMatrix matrix = new BinarySparseMatrix();
				for (JsonNode pair : allPairs) {
					matrix.set(pair.get(0), pair.get(1));
					matrix.set(pair.get(1), pair.get(0));
				}

				warshall(matrix);

				if (emitClusters) {
					Set<JsonNode> remainingRows = new HashSet<JsonNode>(matrix.getRows());
					while (!remainingRows.isEmpty()) {
						ArrayNode cluster = new ArrayNode(null);
						JsonNode row = remainingRows.iterator().next();
						cluster.add(row);
						for (JsonNode column : matrix.get(row))
							cluster.add(column);
						remainingRows.remove(cluster);
					}
				} else {
					for (JsonNode row : matrix.getRows())
						for (JsonNode column : matrix.get(row))
							if (JsonNodeComparator.INSTANCE.compare(row, column) < 0)
								out.collect(key, JsonUtil.asArray(row, column));
				}
			}
		}
	}

	public static class FlattenPairs extends ElementaryOperator {
		public FlattenPairs(JsonStream input) {
			super(input);
		}

		public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				out.collect(value.get(0), NullNode.getInstance());
				out.collect(value.get(1), NullNode.getInstance());
			}
		}
	}

	public static class RemoveDuplicateEntities extends ElementaryOperator {
		public RemoveDuplicateEntities(JsonStream input) {
			super(input);
		}

		public static class Implementation extends SopremoReduce<Key, PactJsonObject, Key, PactJsonObject> {
			@Override
			protected void reduce(JsonNode key, StreamArrayNode values, JsonCollector out) {
				out.collect(NullNode.getInstance(), key);
			}
		}
	}

	public static class SubstituteWithKeyValueList extends CompositeOperator {

		public SubstituteWithKeyValueList(JsonStream input, JsonStream keyValueList) {
			super(input, keyValueList);
		}

		@Override
		public SopremoModule asElementaryOperators() {
			Projection left = new Projection(new ArrayAccess(0), EvaluationExpression.SAME_VALUE, getInput(0));
			ReplaceWithRightInput replacedLeft = new ReplaceWithRightInput(0, left);
			Projection right = new Projection(new ArrayAccess(1), EvaluationExpression.SAME_VALUE, replacedLeft);
			ReplaceWithRightInput replacedRight = new ReplaceWithRightInput(0, right);

			return SopremoModule.valueOf(getName(), replacedRight);
		}

		public static class SwapKeyValue extends ElementaryOperator {
			public SwapKeyValue(JsonStream input) {
				super(input);
			}

			public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
				@Override
				protected void map(JsonNode key, JsonNode value, JsonCollector out) {
					out.collect(value, key);
				}
			}
		}

		public static class ReplaceWithRightInput extends ElementaryOperator {
			private int index;

			public ReplaceWithRightInput(int index, JsonStream input) {
				super(input);
				this.index = index;
			}

			public static class Implementation extends
					SopremoMatch<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
				private int index;

				@Override
				protected void match(JsonNode key, JsonNode value1, JsonNode value2, JsonCollector out) {
					((ArrayNode) value1).set(index, value2);
					out.collect(NullNode.getInstance(), value1);
				}
			}
		}
	}
}
