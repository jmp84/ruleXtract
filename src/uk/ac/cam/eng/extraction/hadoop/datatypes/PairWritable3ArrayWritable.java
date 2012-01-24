/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author jmp84
 */
public class PairWritable3ArrayWritable extends ArrayWritable {

    public PairWritable3ArrayWritable() {
        super(PairWritable3.class);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.ArrayWritable#get()
     */
    @Override
    public PairWritable3[] get() {
        Writable[] values = super.get();
        PairWritable3[] res = new PairWritable3[values.length];
        for (int i = 0; i < values.length; i++) {
            res[i] = (PairWritable3) values[i];
        }
        return res;
    }
}
