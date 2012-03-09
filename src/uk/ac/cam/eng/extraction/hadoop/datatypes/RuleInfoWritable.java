/**
 * 
 */
package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author juan
 *
 */
public class RuleInfoWritable implements WritableComparable<RuleInfoWritable> {

    private IntWritable numberUnalignedSourceWords;
    private IntWritable numberUnalignedTargetWords;
    
    public RuleInfoWritable(Rule r) {
    	numberUnalignedSourceWords = 
    			new IntWritable(r.getNumberUnalignedSourceWords());
    	numberUnalignedTargetWords =
    			new IntWritable(r.getNumberUnalignedTargetWords());
    }
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		numberUnalignedSourceWords.write(out);
		numberUnalignedTargetWords.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		numberUnalignedSourceWords.readFields(in);
		numberUnalignedTargetWords.readFields(in);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(RuleInfoWritable o) {
		int cmp = numberUnalignedSourceWords.compareTo(o.numberUnalignedSourceWords);
		if (cmp != 0) {
			return cmp;
		}
		return numberUnalignedTargetWords.compareTo(o.numberUnalignedTargetWords);
	}
}
