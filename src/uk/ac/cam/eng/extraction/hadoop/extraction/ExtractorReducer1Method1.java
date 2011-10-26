
package uk.ac.cam.eng.extraction.hadoop.extraction;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * This class implements the first reducer used to compute source-to-target and
 * target-to-source probabilities (see method 1 in paper fast, easy and cheap by
 * Chris Dyer et al.)
 */
public class ExtractorReducer1Method1 extends
        Reducer<RuleWritable, IntWritable, RuleWritable, IntWritable> {

    protected void reduce(RuleWritable key, Iterable<IntWritable> values,
            Context context) throws java.io.IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count: values) {
            sum += count.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
