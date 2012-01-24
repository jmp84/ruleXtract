/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author jmp84 This class represents a pair of writables. It is used with the
 *         first element being a marginal RulePatternWritable and the second
 *         element an ArrayWritable containing features
 */

public class PairWritable3Pattern implements
        WritableComparable<PairWritable3Pattern> {

    public RulePatternWritable first;
    public ArrayWritable second;

    public PairWritable3Pattern() {
        first = new RulePatternWritable();
        second = new ArrayWritable(DoubleWritable.class);
    }

    public PairWritable3Pattern(RulePatternWritable first, ArrayWritable second) {
        this.first = first;
        this.second = second;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(first.toString());
        sb.append(" ");
        sb.append(second.toStrings());
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
    public int compareTo(PairWritable3Pattern o) {
        int cmp = first.compareTo(((PairWritable3Pattern) o).first);
        if (cmp != 0) {
            return cmp;
        }
        // comparing hash codes because ArrayWritable doesn't implement
        // WritableComparable
        Integer thisHashCode = second.hashCode();
        return thisHashCode.compareTo(o.second.hashCode());
    }
}
