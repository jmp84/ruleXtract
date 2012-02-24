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
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * @author jmp84 This class implements the mapper (method 3) described in
 *         "fast, easy cheap" by Chris Dyer et al. to compute rule probabilities
 */

public class ExtractorMapperMethod3 extends
        // Mapper<IntWritable, TextArrayWritable, RuleWritable, PairWritable> {
        Mapper<IntWritable, TextArrayWritable, BytesWritable, PairWritable> {

    // private final static RuleWritable rule = new RuleWritable();
    private final static IntWritable one = new IntWritable(1);

    private byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    /**                                                                                                                                                                                                    
     *                                                                                                                                  
     */
    @Override
    protected void
            map(IntWritable key, TextArrayWritable value, Context context)
                    throws java.io.IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // System.err.println("record number: " + key);
        // Get the associated records from the input array
        String sentenceAlign = ((Text) value.get()[0]).toString();
        String wordAlign = ((Text) value.get()[1]).toString();
        // Preprocess the lines
        boolean side1source = conf.getBoolean("side1source", false);
        SentencePair sp = new SentencePair(sentenceAlign, side1source);
        Alignment a = new Alignment(wordAlign, sp, side1source);
        RuleExtractor re = new RuleExtractor(conf);
        boolean source2target = conf.getBoolean("source2target", true);
        for (Rule r: re.extract(a, sp)) {
            // TODO replace this by a write method instead of creating an object
            RuleWritable sourceMarginal =
                    RuleWritable.makeSourceMarginal(r, source2target);
            RuleWritable targetMarginal =
                    RuleWritable.makeTargetMarginal(r, source2target);
            PairWritable sideCountPair = null;
            BytesWritable ruleMarginalBytes = null;
            if (source2target) {
                sideCountPair =
                        new PairWritable(targetMarginal, one);
                ruleMarginalBytes =
                        new BytesWritable(object2ByteArray(sourceMarginal));
            }
            else {
                sideCountPair =
                        new PairWritable(sourceMarginal, one);
                ruleMarginalBytes =
                        new BytesWritable(object2ByteArray(targetMarginal));
            }
            context.write(ruleMarginalBytes, sideCountPair);
        }
    }
}
