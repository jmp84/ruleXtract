/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author jmp84
 */
public class PairWritable3PatternArrayWritable extends ArrayWritable {

    public PairWritable3PatternArrayWritable() {
        super(PairWritable3Pattern.class);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.ArrayWritable#get()
     */
    @Override
    public PairWritable3Pattern[] get() {
        Writable[] values = super.get();
        PairWritable3Pattern[] res = new PairWritable3Pattern[values.length];
        for (int i = 0; i < values.length; i++) {
            res[i] = (PairWritable3Pattern) values[i];
        }
        return res;
    }

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();
		PairWritable3Pattern[] elements = get();
		if (elements.length > 0) {
			res.append(elements[0].toString());
		}
		for (int i = 1; i < elements.length; i++) {
			res.append(" " + elements[i].toString());
		}
		return res.toString();
	}
}
