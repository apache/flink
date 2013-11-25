package eu.stratosphere.pact.common.io.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Base64;

import eu.stratosphere.pact.common.io.TableInputFormat;

/**
 * Utility for {@link TableInputFormat}
 */
public class HBaseUtil {

	/**
	 * Writes the given scan into a Base64 encoded string.
	 * 
	 * @param scan
	 *        The scan to write out.
	 * @return The scan saved in a Base64 encoded string.
	 * @throws IOException
	 *         When writing the scan fails.
	 */
	static String convertScanToString(Scan scan) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		scan.write(dos);
		return Base64.encodeBytes(out.toByteArray());
	}

	/**
	 * Converts the given Base64 string back into a Scan instance.
	 * 
	 * @param base64
	 *        The scan details.
	 * @return The newly created Scan instance.
	 * @throws IOException
	 *         When reading the scan instance fails.
	 */
	public static Scan convertStringToScan(String base64) throws IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(base64));
		DataInputStream dis = new DataInputStream(bis);
		Scan scan = new Scan();
		scan.readFields(dis);
		return scan;
	}
}
