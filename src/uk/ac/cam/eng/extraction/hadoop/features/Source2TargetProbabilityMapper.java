/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Mapper to compute source-to-target probability. Uses method 3
 *         descried in "Fast, easy, cheap, etc." by Chris Dyer et al.
 */
public class Source2TargetProbabilityMapper extends
        Mapper<RuleWritable, RuleInfoWritable, RuleWritable, PairWritable> {

    // static writables to avoid memory consumption
    private final static IntWritable one = new IntWritable(1);
    private static RuleWritable sourceMarginal = new RuleWritable();
    private static RuleWritable targetMarginal = new RuleWritable();
    private static PairWritable targetAndCount = new PairWritable();

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void
            map(RuleWritable key, RuleInfoWritable value, Context context)
                    throws IOException, InterruptedException {
        sourceMarginal.setSource(key.getSource());
        targetMarginal.setTarget(key.getTarget());
        targetAndCount.set(targetMarginal, one);
        context.write(sourceMarginal, targetAndCount);
    }
}