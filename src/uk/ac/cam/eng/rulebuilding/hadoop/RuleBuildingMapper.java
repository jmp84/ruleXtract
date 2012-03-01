/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.rulebuilding.retrieval.RuleFileBuilder;

/**
 * @author juan
 */
public class RuleBuildingMapper extends
        Mapper<IntWritable, RuleWritable, PairWritable3, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private RuleFileBuilder ruleFileBuilder;

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
     * Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        RuleFileBuilder ruleFileBuilder = new RuleFileBuilder(conf);
    }

    /**                                                                                                                                                                                                    
     *                                                                                                                                  
     */
    @Override
    protected void map(IntWritable key, RuleWritable value, Context context)
            throws java.io.IOException, InterruptedException {
        List<PairWritable3> rules = ruleFileBuilder.getRules(value);
        for (PairWritable3 rule: rules) {
            context.write(rule, one);
        }
    }
}
