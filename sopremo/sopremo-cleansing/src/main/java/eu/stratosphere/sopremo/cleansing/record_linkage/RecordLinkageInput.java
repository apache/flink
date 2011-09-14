package eu.stratosphere.sopremo.cleansing.record_linkage;

import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class RecordLinkageInput implements JsonStream, Cloneable {
	/**
	 * 
	 */
	private final Operator recordLinkage;

	private final int index;

	private EvaluationExpression idProjection = EvaluationExpression.VALUE;

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;
	
	private Output source;

	RecordLinkageInput(Operator recordLinkage, int index) {
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
	public Output getSource() {
		if(source != null)
			return this.source;
		
		return this.recordLinkage.getInput(index);
	}
	
public void setSource(Output source) {
	if (source == null)
		throw new NullPointerException("source must not be null");

	this.source = source;
}

	public EvaluationExpression getIdProjection() {
		return idProjection;
	}

	public void setIdProjection(EvaluationExpression idProjection) {
		if (idProjection == null)
			throw new NullPointerException("idProjection must not be null");

		this.idProjection = idProjection;
	}

	public EvaluationExpression getResultProjection() {
		return resultProjection;
	}

	public void setResultProjection(EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}

	@Override
	public String toString() {
		return String.format("RecordLinkageInput [index=%s, idProjection=%s, resultProjection=%s]", index,
			idProjection, resultProjection);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + idProjection.hashCode();
		result = prime * result + index;
		result = prime * result + resultProjection.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		RecordLinkageInput other = (RecordLinkageInput) obj;
		return index == other.index && idProjection.equals(other.idProjection)
			&& resultProjection.equals(other.resultProjection);
	}
}