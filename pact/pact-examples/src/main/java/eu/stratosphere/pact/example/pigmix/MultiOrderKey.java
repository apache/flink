package eu.stratosphere.pact.example.pigmix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;

public class MultiOrderKey implements Key {

    public String query_term;
    int timespent;
    double estimated_revenue;

    public MultiOrderKey() {
        query_term = null;
        timespent = 0;
        estimated_revenue = 0.0;
    }

    public MultiOrderKey(String qt, String ts, String er) {
        query_term = qt.toString();
        try {
            timespent = Integer.valueOf(ts.toString());
        } catch (NumberFormatException nfe) {
            timespent = 0;
        }
        try {
            estimated_revenue = Double.valueOf(er.toString());
        } catch (NumberFormatException nfe) {
            estimated_revenue = 0.0;
        }
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(timespent);
        out.writeDouble(estimated_revenue);
        out.writeInt(query_term.length());
        out.writeBytes(query_term);
    }
	@Override
    public void read(DataInput in) throws IOException {
        timespent = in.readInt();
        estimated_revenue = in.readDouble();
        int len = in.readInt();
        byte[] b = new byte[len];
        in.readFully(b);
        query_term = new String(b);
    }

	@Override
    public int compareTo(final Key other) {
		if(!(other instanceof MultiOrderKey)){
			return 0;
		}
		final MultiOrderKey mokother = (MultiOrderKey) other;
        int rc = query_term.compareTo(mokother.query_term);
        if (rc != 0) return rc;
        if (estimated_revenue < mokother.estimated_revenue) return 1;
        else if (estimated_revenue > mokother.estimated_revenue) return -1;
        if (timespent < mokother.timespent) return -1;
        else if (timespent > mokother.timespent) return 1;
        return 0;
    }

}