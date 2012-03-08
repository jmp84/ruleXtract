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
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * @author jmp84 Mapper for rule extraction. Simply extracts the rule and writes
 *         them to disk. The output will be the input to mapreduce features.
 */
public class ExtractorMapper extends
        Mapper<IntWritable, TextArrayWritable, RuleWritable, IntWritable> {

    private static IntWritable one = new IntWritable(1);

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
            context.write(rw, one);
        }
    }
}
