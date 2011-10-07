package eu.stratosphere.sopremo.cleansing.record_linkage;

import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class RecordLinkageInput implements JsonStream, Cloneable {
	/**
	 * 
	 */
	private final Operator<?> recordLinkage;

	private final int index;

	private EvaluationExpression idProjection = EvaluationExpression.VALUE;

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	private Operator<?>.Output source;

	RecordLinkageInput(Operator<?> recordLinkage, int index) {
		this.recordLinkage = recordLinkage;
		this.index = index;
	}

	@Override
	protected RecordLinkageInput clone() {
		try {
			return (RecordLinkageInput) super.clone();
		} catch (CloneNotSupportedException e) {
			// cannot happen
			return null;
		}
	}

	@Override
	public Operator<?>.Output getSource() {
		if (this.source != null)
			return this.source;

		return this.recordLinkage.getInput(this.index).getSource();
	}

	public void setSource(Operator<?>.Output source) {
		if (source == null)
			throw new NullPointerException("source must not be null");

		this.source = source;
	}

	public EvaluationExpression getIdProjection() {
		return this.idProjection;
	}

	public void setIdProjection(EvaluationExpression idProjection) {
		if (idProjection == null)
			throw new NullPointerException("idProjection must not be null");

		this.idProjection = idProjection;
	}

	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	public void setResultProjection(EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}

	@Override
	public String toString() {
		return String.format("RecordLinkageInput [index=%s, idProjection=%s, resultProjection=%s]", this.index,
			this.idProjection, this.resultProjection);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.idProjection.hashCode();
		result = prime * result + this.index;
		result = prime * result + this.resultProjection.hashCode();
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

		RecordLinkageInput other = (RecordLinkageInput) obj;
		return this.index == other.index && this.idProjection.equals(other.idProjection)
			&& this.resultProjection.equals(other.resultProjection);
	}
}