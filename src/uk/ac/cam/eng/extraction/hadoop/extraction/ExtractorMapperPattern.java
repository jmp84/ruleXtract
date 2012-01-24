/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritablePattern;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RulePatternWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;
import uk.ac.cam.eng.rulebuilding.retrieval.RulePattern;

/**
 * @author jmp84 This class implements the mapper (method 3) described in
 *         "fast, easy cheap" by Chris Dyer et al. to compute pattern
 *         probability. It's the same as for rules except that we replace rules
 *         by their patterns.
 */

public class ExtractorMapperPattern
        extends
        Mapper<IntWritable, TextArrayWritable, RulePatternWritable, PairWritablePattern> {

    private final static IntWritable one = new IntWritable(1);

    /**                                                                                                                                                                                                    
     *                                                                                                                                  
     */
    @Override
    protected void
            map(IntWritable key, TextArrayWritable value, Context context)
                    throws java.io.IOException, InterruptedException {
        // Get the associated records from the input array
        String sentenceAlign = ((Text) value.get()[0]).toString();
        String wordAlign = ((Text) value.get()[1]).toString();

        // Preprocess the lines
        SentencePair sp = new SentencePair(sentenceAlign, false);
        Alignment a = new Alignment(wordAlign, sp, false); // TODO replace 2 by
                                                           // side, get side
                                                           // from a config file
        Configuration conf = context.getConfiguration();
        RuleExtractor re = new RuleExtractor(conf);
        boolean source2target = conf.getBoolean("source2target", true);
        for (Rule r: re.extract(a, sp)) {
            RulePattern pattern = RulePattern.getPattern(r);
            RulePatternWritable sourceMarginal =
                    RulePatternWritable.makeSourceMarginal(pattern);
            RulePatternWritable targetMarginal =
                    RulePatternWritable.makeTargetMarginal(pattern);
            PairWritablePattern sideCountPair = null;
            if (source2target) {
                sideCountPair =
                        new PairWritablePattern(targetMarginal, one);
                context.write(sourceMarginal, sideCountPair);
            }
            else {
                sideCountPair =
                        new PairWritablePattern(sourceMarginal, one);
                context.write(targetMarginal, sideCountPair);
            }
        }
    }
}
