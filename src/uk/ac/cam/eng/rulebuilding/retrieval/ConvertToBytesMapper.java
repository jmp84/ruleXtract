/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable2;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * @author jmp84 This class is a mapper in a mapreduce job that converts a
 *         SequenceFile of RuleWritable and DoubleWritable to a SequenceFile of
 *         BytesWritable and BytesWritable
 */
public class ConvertToBytesMapper extends
        Mapper<RuleWritable, DoubleWritable, BytesWritable, PairWritable2> {

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
    protected void map(RuleWritable key, DoubleWritable value, Context context)
            throws java.io.IOException, InterruptedException {
        RuleWritable sourceMarginal = RuleWritable.makeSourceMarginal(key);
        RuleWritable targetMarginal = RuleWritable.makeTargetMarginal(key);
        PairWritable2 targetAndProb = new PairWritable2(targetMarginal, value);
        BytesWritable keyBytes = new BytesWritable(
                object2ByteArray(sourceMarginal));
        // BytesWritable valueBytes = new
        // BytesWritable(object2ByteArray(value));
        // context.write(keyBytes, valueBytes);
        context.write(keyBytes, targetAndProb);
    }
}
