/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

// TODO make this run problems at runtime

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author jmp84 This class represents a pair of writables. It is used with the
 *         first element being a marginal RuleWritable and the second element a
 *         IntWritable
 */

// here T extends WritableComparable rather than WritableComparable<T> because
// this is what the IntWritable
// class does.
public class PairWritableFlexible<S extends WritableComparable<S>, T extends WritableComparable>
        implements WritableComparable<PairWritableFlexible<S, T>> {

    // public class PairWritable implements WritableComparable<PairWritable> {

    // public RuleWritable first;
    public S first;
    // public IntWritable second;
    public T second;

    /*
     * don't need this constructor public PairWritable() { first = new
     * RuleWritable(); second = new IntWritable(); }
     */

    // public PairWritable(RuleWritable first, IntWritable second) {
    // this.first = first;
    // this.second = second;
    // }

    public PairWritableFlexible(S first, T second) {
        this.first = first;
        this.second = second;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(first.toString());
        sb.append(" ");
        // sb.append(second.get());
        sb.append(second.toString());
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
    public int compareTo(PairWritableFlexible<S, T> o) {
        // int cmp = first.compareTo(((PairWritable)o).first);
        int cmp = first.compareTo(o.first);
        if (cmp != 0) {
            return cmp;
        }
        // return second.compareTo(((PairWritable)o).first); BIG BUG!
        return second.compareTo(o.second);
    }

}
