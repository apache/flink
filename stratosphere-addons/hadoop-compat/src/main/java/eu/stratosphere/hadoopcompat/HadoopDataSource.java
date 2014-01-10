package eu.stratosphere.hadoopcompat;

import java.lang.annotation.Annotation;

import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.util.Visitor;

public class HadoopDataSource extends GenericDataSource {

	@Override
	public void addStubAnnotation(Class<? extends Annotation> clazz) {
		// TODO Auto-generated method stub

	}

	@Override
	public void accept(Visitor<Operator> visitor) {
		// TODO Auto-generated method stub

	}

}
