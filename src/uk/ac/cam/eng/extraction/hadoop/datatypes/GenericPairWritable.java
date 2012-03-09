/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.util.Pair;

/**
 * @author jmp84
 */
public class GenericPairWritable extends Pair<Writable, Writable> implements
        Writable {

    Class<? extends Writable> clazz;

    public GenericPairWritable(Class<? extends Writable> clazz)
            throws InstantiationException, IllegalAccessException {
        this.clazz = clazz;
        first = clazz.newInstance();
        second = clazz.newInstance();
    }

    public class IntPairWritable extends GenericPairWritable {

        public IntPairWritable() throws InstantiationException,
                IllegalAccessException {
            super(IntWritable.class);
        }
    }

    private Writable first;
    private Writable second;

    public GenericPairWritable() {}

    /**
     * @param first
     * @param second
     */
    public GenericPairWritable(T first, U second) {
        this.first = first;
        this.second = second;
    }

    /**
     * @return the first
     */
    public T getFirst() {
        return first;
    }

    /**
     * @param first
     *            the first to set
     */
    public void setFirst(T first) {
        this.first = first;
    }

    /**
     * @return the second
     */
    public U getSecond() {
        return second;
    }

    /**
     * @param second
     *            the second to set
     */
    public void setSecond(U second) {
        this.second = second;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
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
