/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author jmp84 This class represents a pair of writables. It is used with the
 *         first element being a marginal RuleWritable and the second element an
 *         ArrayWritable containing features
 */

// TODO use generics (so far runtime error when using generics)

// here T extends WritableComparable rather than WritableComparable<T> because
// this is what the DoubleWritable
// class does.
public class PairWritable3 implements WritableComparable<PairWritable3> {

    public RuleWritable first;
    public ArrayWritable second;

    public PairWritable3() {
        first = new RuleWritable();
        second = new ArrayWritable(DoubleWritable.class);
    }

    public PairWritable3(RuleWritable first, ArrayWritable second) {
        this.first = first;
        this.second = second;
    }
    
    public PairWritable3 copy() {
    	RuleWritable f =
    			new RuleWritable(RuleWritable.makeSourceMarginal(first, true),
    					RuleWritable.makeTargetMarginal(first, true));
    	ArrayWritable s = new ArrayWritable(DoubleWritable.class);
    	s.set(second.get());
    	return new PairWritable3(f, s);
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(first.toString());
        sb.append(" ");
        sb.append(second.toStrings());
        return sb.toString();
    }

    /**
     * Prints in HiFST format (X, V nonterminal, etc.)
     * 
     * @return a rule with features in HiFST format
     */
    public String toStringShallow() {
        StringBuilder sb = new StringBuilder();
        sb.append(first.toStringShallow());
        Writable[] features = second.get();
        for (Writable w: features) {
            sb.append(" " + w.toString());
        }
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
    public int compareTo(PairWritable3 o) {
        int cmp = first.compareTo(((PairWritable3) o).first);
        if (cmp != 0) {
            return cmp;
        }
        // comparing hash codes because ArrayWritable doesn't implement
        // WritableComparable
        Integer thisHashCode = second.hashCode();
        return thisHashCode.compareTo(o.second.hashCode());
    }
}
