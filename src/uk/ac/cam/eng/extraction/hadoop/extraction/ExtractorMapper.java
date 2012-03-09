/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * @author jmp84 Mapper for rule extraction. Extracts the rules and writes
 *         the rule and additional info (unaligned words, etc.). We separate
 *         the rule from its additional info to be flexible and avoid equality
 *         problems whenever we add more info to the rule.
 *         The output will be the input to mapreduce features.
 */
public class ExtractorMapper extends
        Mapper<IntWritable, TextArrayWritable, RuleWritable, RuleInfoWritable> {

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void
            map(IntWritable key, TextArrayWritable value, Context context)
                    throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String sentenceAlign = ((Text) value.get()[0]).toString();
        String wordAlign = ((Text) value.get()[1]).toString();
        boolean side1source = conf.getBoolean("side1source", false);
        SentencePair sp = new SentencePair(sentenceAlign, side1source);
        Alignment a = new Alignment(wordAlign, sp, side1source);
        RuleExtractor re = new RuleExtractor(conf);
        for (Rule r: re.extract(a, sp)) {
            RuleWritable rw = new RuleWritable(r);
            RuleInfoWritable riw = new RuleInfoWritable(r);
            context.write(rw, riw);
        }
    }
}
