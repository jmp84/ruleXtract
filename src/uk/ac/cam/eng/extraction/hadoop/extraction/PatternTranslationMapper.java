/**
 * 
 */
package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3ArrayWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritablePattern;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RulePatternWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;
import uk.ac.cam.eng.rulebuilding.retrieval.RulePattern;

/**
 * @author juan
 * This class is a mapper for the pattern translation feature. It reads the rules
 * and base features (s2t prob, t2s prob, count) computed in extraction and compute
 * probabilities for the patterns
 */
public class PatternTranslationMapper extends Mapper<BytesWritable, PairWritable3ArrayWritable, RulePatternWritable, PairWritablePattern> {
	
    private static IntWritable patternCount = new IntWritable();
	
    private static RuleWritable convertValueBytes(byte[] bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes, bytes.length);
        RuleWritable value = new RuleWritable();
        try {
            value.readFields(in);
        }
        catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }
    
    /**                                                                                                                                                                                                    
     *                                                                                                                                  
     */
    @Override
    protected void map(BytesWritable key, PairWritable3ArrayWritable value, Context context) throws java.io.IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        boolean source2target = conf.getBoolean("source2target", true);
        // we assume we read the output of the source-to-target extraction job
        PairWritable3[] targetsAndBaseFeatures = value.get();
        RuleWritable source = convertValueBytes(key.getBytes());
        for (PairWritable3 targetAndBaseFeatures: targetsAndBaseFeatures) {
        	Rule r = new Rule(source, targetAndBaseFeatures.first);
        	int count = (int)((DoubleWritable)targetAndBaseFeatures.second.get()[2]).get();
        	patternCount.set(count);
            RulePattern pattern = RulePattern.getPattern(r);
            RulePatternWritable sourceMarginal =
                    RulePatternWritable.makeSourceMarginal(pattern);
            RulePatternWritable targetMarginal =
                    RulePatternWritable.makeTargetMarginal(pattern);
            PairWritablePattern sideCountPair = null;
            if (source2target) {
                sideCountPair =
                        new PairWritablePattern(targetMarginal, patternCount);
                context.write(sourceMarginal, sideCountPair);
            }
            else {
                sideCountPair =
                        new PairWritablePattern(sourceMarginal, patternCount);
                context.write(targetMarginal, sideCountPair);
            }
        }
    }
}
