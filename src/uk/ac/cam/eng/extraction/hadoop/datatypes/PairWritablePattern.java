/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author jmp84 This class represents a pair of writables. It is used with the
 *         first element being a marginal RulePatternWritable and the second
 *         element a IntWritable
 */

public class PairWritablePattern implements
        WritableComparable<PairWritablePattern> {

    public RulePatternWritable first;
    public IntWritable second;

    public PairWritablePattern() {
        first = new RulePatternWritable();
        second = new IntWritable();
    }

    public PairWritablePattern(RulePatternWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(first.toString());
        sb.append(" ");
        sb.append(second.get());
        return sb.toString();
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
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(PairWritablePattern o) {
        int cmp = first.compareTo(((PairWritablePattern) o).first);
        if (cmp != 0) {
            return cmp;
        }
        // return second.compareTo(((PairWritable)o).first); BIG BUG!
        return second.compareTo(o.second);
    }
}
