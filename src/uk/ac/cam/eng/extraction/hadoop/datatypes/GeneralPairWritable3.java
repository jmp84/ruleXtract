/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author jmp84 TODO fix this issue of having many pair classes
 */
public class GeneralPairWritable3 implements Writable {

    private RuleWritable first;
    private SortedMapWritable second;

    public GeneralPairWritable3() {
        first = new RuleWritable();
        second = new SortedMapWritable();
    }

    /**
     * This constructor takes a sorted map because it is used in the MapReduce
     * feature reducer to build the output value
     * 
     * @param first
     * @param second
     */
    public GeneralPairWritable3(RuleWritable first, SortedMapWritable second) {
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
    public SortedMapWritable getSecond() {
        return second;
    }

    /**
     * @param second
     *            the second to set
     */
    public void setSecond(SortedMapWritable second) {
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
