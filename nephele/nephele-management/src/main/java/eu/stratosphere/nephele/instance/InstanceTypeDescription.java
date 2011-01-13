package eu.stratosphere.nephele.instance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * An instance type description provides details of instance type. Is can comprise both the hardware description from
 * the instance type description (as provided by the operator/administrator of the instance) as well as the actual
 * hardware description which has been determined on the compute instance itself.
 * 
 * @author warneke
 */
public class InstanceTypeDescription implements IOReadableWritable {

	/**
	 * The instance type.
	 */
	private InstanceType instanceType = null;

	/**
	 * The hardware description as created by the {@link InstanceManager}.
	 */
	private HardwareDescription hardwareDescription = null;

	/**
	 * The number of available instances of this type.
	 */
	private int numberOfAvailableInstances = 0;

	/**
	 * Public default constructor required for serialization process.
	 */
	public InstanceTypeDescription() {
	}

	/**
	 * Constructs a new instance type description
	 * 
	 * @param instanceType
	 *        the instance type
	 * @param hardwareDescription
	 *        the hardware description as created by the {@link InstanceManager}
	 * @param numberOfAvailableInstances
	 *        the number of available instances of this type
	 */
	InstanceTypeDescription(InstanceType instanceType, HardwareDescription hardwareDescription,
			int numberOfAvailableInstances) {

		this.instanceType = instanceType;
		this.hardwareDescription = hardwareDescription;
		this.numberOfAvailableInstances = numberOfAvailableInstances;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		if (this.instanceType == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.instanceType.write(out);
		}

		if (this.hardwareDescription == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.hardwareDescription.write(out);
		}

		out.writeInt(this.numberOfAvailableInstances);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {
		
		if(in.readBoolean()) {
			this.instanceType = new InstanceType();
			this.instanceType.read(in);
		} else {
			this.instanceType = null;
		}
		
		if(in.readBoolean()) {
			this.hardwareDescription = new HardwareDescription();
			this.hardwareDescription.read(in);
		}

		this.numberOfAvailableInstances = in.readInt();
	}

}
