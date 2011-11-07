package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

@Name(verb = "extract from")
public class EntityExtraction extends CompositeOperator<EntityExtraction> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5817110603520085487L;

	private ObjectCreation projections = new ObjectCreation();

	public ObjectCreation getProjections() {
		return this.projections;
	}

	@Property
	@Name(preposition = "into")
	public void setProjections(ObjectCreation projections) {
		if (projections == null)
			throw new NullPointerException("projection must not be null");

		this.projections = projections;
	}

	public EntityExtraction withProjections(ObjectCreation projection) {
		this.setProjections(projection);
		return this;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		return SopremoModule.valueOf(this.getName());
	}

}
