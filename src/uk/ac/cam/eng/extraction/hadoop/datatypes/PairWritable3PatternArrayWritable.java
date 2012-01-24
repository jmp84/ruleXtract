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
}
