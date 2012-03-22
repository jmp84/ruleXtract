/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RulePatternWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Mapper to compute the source-to-target pattern probability.
 *         Emits the rule with a count of one, the pattern with a count of one
 *         and the source pattern with a count of one. The partitioner, the
 *         sorting comparator and the grouping comparator are modified to have
 *         all rules with the same source pattern processed by the same reducer
 *         with the source pattern being the smallest element followed by
 *         pattern, followed by the rules.
 */
public class Source2TargetPatternProbabilityMapper extends
        Mapper<RuleWritable, RuleInfoWritable, RuleWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void
            map(RuleWritable key, RuleInfoWritable value, Context context)
                    throws IOException, InterruptedException {
        context.write(key, one);
        RulePatternWritable pattern = new RulePatternWritable(key);
        context.write(pattern, one);
        RulePatternWritable sourcePattern = pattern.makeSourceMarginal();
        context.write(sourcePattern, one);
    }
}
