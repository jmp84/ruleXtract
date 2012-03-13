/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author jmp84
 */
public class GeneralPairWritable2 implements Writable {

    private RuleWritable first;
    private MapWritable second;

    public GeneralPairWritable2() {
        first = new RuleWritable();
        second = new MapWritable();
    }

    /**
     * @param first
     * @param second
     */
    public GeneralPairWritable2(RuleWritable first, MapWritable second) {
        this.first = first;
        this.second = second;
    }

    /**
     * @return the first
     */
    public RuleWritable getFirst() {
        return first;
    }

    /**
     * @param first
     *            the first to set
     */
    public void setFirst(RuleWritable first) {
        this.first = first;
    }

    /**
     * @return the second
     */
    public MapWritable getSecond() {
        return second;
    }

    /**
     * @param second
     *            the second to set
     */
    public void setSecond(MapWritable second) {
        this.second = second;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
}
