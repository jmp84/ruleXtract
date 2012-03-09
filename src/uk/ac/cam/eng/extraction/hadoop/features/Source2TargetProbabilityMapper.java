/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Mapper to compute source-to-target probability. Uses method 3 
 * descried in "Fast, easy, cheap, etc." by Chris Dyer et al.
 */
public class Source2TargetProbabilityMapper extends
		Mapper<RuleWritable, RuleInfoWritable, RuleWritable, PairWritable> {

	private static IntWritable one = new IntWritable(1);
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(RuleWritable key, RuleInfoWritable value, Context context)
			throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        RuleWritable sourceMarginal = RuleWritable.makeSourceMarginal(key);
        RuleWritable targetMarginal = RuleWritable.makeTargetMarginal(key);
        PairWritable targetAndCount = new PairWritable(targetMarginal, one);
        context.write(sourceMarginal, targetAndCount);
	}
}
