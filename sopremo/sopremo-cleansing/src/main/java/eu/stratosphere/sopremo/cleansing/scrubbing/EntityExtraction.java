package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

@Name(verb = "extract from")
@InputCardinality(min = 1, max = 1)
@OutputCardinality(min = 0, max = Integer.MAX_VALUE)
public class EntityExtraction extends CompositeOperator<EntityExtraction> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5817110603520085487L;

	private ObjectCreation extractions = new ObjectCreation();

	public ObjectCreation getExtractions() {
		return this.extractions;
	}

	@Property
	@Name(preposition = "into")
	public void setExtractions(ObjectCreation extractions) {
		if (extractions == null)
			throw new NullPointerException("projection must not be null");

		this.extractions = extractions;
	}

	public EntityExtraction withProjections(ObjectCreation projection) {
		this.setExtractions(projection);
		return this;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		SopremoModule module = new SopremoModule(this.getName(), 1, this.extractions.getMappingSize());
		return module;
	}

}
