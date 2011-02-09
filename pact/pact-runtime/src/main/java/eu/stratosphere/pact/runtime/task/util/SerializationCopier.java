package eu.stratosphere.pact.runtime.task.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * Creates deep copies of {@link IOReadableWritable} objects.
 * Objects are serialized into a byte array and later deserialized from there. 
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 * @param <T>
 */
public class SerializationCopier<T extends IOReadableWritable> {

	private byte[] serializedCopy;
	
	// streams for reading and writing to byte array
	private ByteArrayOutputStream baos;
	private DataOutputStream dos;
	private ByteArrayInputStream bais;
	private DataInputStream dis;
	
	/**
	 * Creates a SerializationCopier with an byte array of initial size 1024 byte.
	 */
	public SerializationCopier() {
		serializedCopy = new byte[1024];
		baos = new ByteArrayOutputStream();
		dos = new DataOutputStream(baos);
		bais = new ByteArrayInputStream(serializedCopy);
		dis = new DataInputStream(bais);
	}
	
	/**
	 * Creates a SerializationCopier with an byte array of specified size. 
	 * 
	 * @param initialCopySize Initial size of the byte array that stores the serialized copy.
	 */
	public SerializationCopier(int initialCopySize) {
		serializedCopy = new byte[initialCopySize];
		baos = new ByteArrayOutputStream();
		dos = new DataOutputStream(baos);
		bais = new ByteArrayInputStream(serializedCopy);
		dis = new DataInputStream(bais);
	}
	
	/**
	 * Resets the array and write a serialized copy to the array.
	 * If the array is not sufficiently large, a new array is created.
	 * The new size is double of the required size for the current copy.
	 * 
	 * @param copy Object from which a serialized copy is created.
	 */
	public void setCopy(T copy) {
		
		try {
			copy.write(dos);
			dos.flush();
			baos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(baos.size() <= serializedCopy.length) {
			// copy
			System.arraycopy(baos.toByteArray(), 0, serializedCopy, 0, baos.size());
		} else {
			// allocate larger array
			serializedCopy = new byte[baos.size()*2];
			System.arraycopy(baos.toByteArray(), 0, serializedCopy, 0, baos.size());
			// create new input streams
			bais = new ByteArrayInputStream(serializedCopy);
			dis = new DataInputStream(bais);
		}
		baos.reset();		
	}
	
	/**
	 * Deserializes the copy into the provided instance.
	 * 
	 * @param newCopy Instance in which the copy is deserialized.
	 */
	public void getCopy(T newCopy) {
		
		try {
			newCopy.read(dis);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		bais.reset();

	}
	
}
