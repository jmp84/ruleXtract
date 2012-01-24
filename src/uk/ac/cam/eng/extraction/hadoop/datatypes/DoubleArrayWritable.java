/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author jmp84
 */
public class DoubleArrayWritable extends ArrayWritable {

    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.ArrayWritable#get()
     */
    @Override
    public DoubleWritable[] get() {
        Writable[] values = super.get();
        DoubleWritable[] res = new DoubleWritable[values.length];
        for (int i = 0; i < values.length; i++) {
            res[i] = (DoubleWritable) values[i];
        }
        return res;
    }

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();
		DoubleWritable[] elements = get();
		if (elements.length > 0) {
			res.append(elements[0].toString());
		}
		for (int i = 1; i < elements.length; i++) {
			res.append(" " + elements[i].toString());
		}
		return res.toString();
	}
}
