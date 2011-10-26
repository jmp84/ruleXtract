/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * @author jmp84 This class is the 2nd mapper in method 1 for computing
 *         source-to-target and target-to-source probability in the paper
 *         "fast, easy and cheap" by dyer et al.
 */
// TODO replace Text by RuleWritable
public class ExtractorMapper2Method1 extends
        Mapper<RuleWritable, IntWritable, RuleWritable, IntWritable> {

    /**
	 * 
	 */
    @Override
    protected void map(RuleWritable key, IntWritable value, Context context)
            throws java.io.IOException, InterruptedException {

        RuleWritable src = new RuleWritable();
        src.setSource(key.getSource());

        context.write(src, value);
    }

}
