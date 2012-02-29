/**
 * 
 */
package uk.ac.cam.eng.rulebuilding.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;
import uk.ac.cam.eng.rulebuilding.retrieval.RuleFileBuilder;

/**
 * @author juan
 *
 */
public class RuleBuildingMapper extends Mapper<??,??,??,??> {
	
    /**                                                                                                                                                                                                    
     *                                                                                                                                  
     */
    @Override
    protected void map(IntWritable key, RuleWritable value, Context context) throws java.io.IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        RuleFileBuilder ruleFileBuilder = new RuleFileBuilder(conf);
        List<PairWritable3> rules = ruleFileBuilder.getRules(sourceRule, hfile);
        for (PairWritable3 rule: rules) {
            context.write(rule, one);
        }
    }
}
