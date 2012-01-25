package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.Iterator;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;

@Name(verb = "cluster records")
public class RecordLinkageCluster extends CompositeOperator<RecordLinkageCluster> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6651979324544876320L;

	private EvaluationExpression resultExpression = EvaluationExpression.VALUE;

	private BooleanExpression duplicateCondition = new ComparativeExpression(new InputSelection(0),
		BinaryOperator.EQUAL, new InputSelection(1));

	private EvaluationExpression partitionStrategies = new ArrayCreation();

	@Override
	public SopremoModule asElementaryOperators() {
		RecordLinkage<?> rlOp = this.getInputs().size() == 1 ? new IntraSourceRecordLinkage()
			: new InterSourceRecordLinkage();
		rlOp.
			withLinkageMode(LinkageMode.ALL_CLUSTERS_PROVENANCE).
			withAlgorithm(this.inferAlgorithm()).
			withDuplicateCondition(this.duplicateCondition);
		return SopremoModule.valueOf("RL cluster",
			new Projection().withInputs(rlOp).withValueTransformation(this.resultExpression));
	}

	public EvaluationExpression getResultExpression() {
		return this.resultExpression;
	}

	@Property
	@Name(preposition = "into")
	public void setResultExpression(EvaluationExpression resultExpression) {
		if (resultExpression == null)
			throw new NullPointerException("resultExpression must not be null");

		this.resultExpression = resultExpression;
	}

	public BooleanExpression getDuplicateCondition() {
		return this.duplicateCondition;
	}

	@Property
	@Name(preposition = "where")
	public void setDuplicateCondition(BooleanExpression duplicateCondition) {
		if (duplicateCondition == null)
			throw new NullPointerException("duplicateCondition must not be null");

		this.duplicateCondition = duplicateCondition;
	}

	public EvaluationExpression getPartitionStrategies() {
		return this.partitionStrategies;
	}

	@Property
	@Name(preposition = "partition on")
	public void setPartitionStrategies(EvaluationExpression partitionStrategies) {
		if (partitionStrategies == null)
			throw new NullPointerException("partitionStrategies must not be null");

		this.partitionStrategies = partitionStrategies;
	}

	protected RecordLinkageAlgorithm inferAlgorithm() {
		Iterator<EvaluationExpression> partitions = this.partitionStrategies.iterator();
		if (!partitions.hasNext())
			return new Naive();
		DisjunctPartitioning partitioning = new DisjunctPartitioning(partitions.next());
		while (partitions.hasNext())
			partitioning.addPass(partitions.next());
		return partitioning;
	}
}
