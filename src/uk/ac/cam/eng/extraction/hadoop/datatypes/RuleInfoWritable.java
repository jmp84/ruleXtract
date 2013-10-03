/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 Additional info about a rule that is used to build mapreduce
 *         features. The RuleInfoWritable is the output value in the extraction
 *         mapper and the input key in a mapreduce feature mapper. We don't need
 *         to make this class implement WritableComparable because it should
 *         never be used as an input key to a reducer.
 */
public class RuleInfoWritable implements Writable {

    private IntWritable numberUnalignedSourceWords;
    private IntWritable numberUnalignedTargetWords;
    /**
     * Records the different provenances the rule was extracted from. The
     * MapWritable emulates a Set by having values being NullWritable. We do not
     * use AbstractMapWritable to avoid casts and we do not use Map<Writable,
     * Writable> because it doesn't have the readFields and writeFields methods.
     * it is important to use a MapWritable as opposed to a SortedMapWritable
     * for speed
     */
    private MapWritable provenance;

    public RuleInfoWritable() {
        numberUnalignedSourceWords = new IntWritable();
        numberUnalignedTargetWords = new IntWritable();
        provenance = new MapWritable();
    }

    public RuleInfoWritable(Rule r) {
        numberUnalignedSourceWords =
                new IntWritable(r.getNumberUnalignedSourceWords());
        numberUnalignedTargetWords =
                new IntWritable(r.getNumberUnalignedTargetWords());
    }

    public void setProvenance(MapWritable provenance) {
        this.provenance = provenance;
    }

    public MapWritable getProvenance() {
        return provenance;
    }

    public int getNumberUnalignedSourceWords() {
        return numberUnalignedSourceWords.get();
    }

    public int getNumberUnalignedTargetWords() {
        return numberUnalignedTargetWords.get();
    }

    public boolean hasProvenance(String prov) {
        Text key = new Text(prov);
        if (provenance.containsKey(key)) {
            return true;
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        numberUnalignedSourceWords.write(out);
        numberUnalignedTargetWords.write(out);
        provenance.write(out);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        numberUnalignedSourceWords.readFields(in);
        numberUnalignedTargetWords.readFields(in);
        provenance.readFields(in);
    }
}
