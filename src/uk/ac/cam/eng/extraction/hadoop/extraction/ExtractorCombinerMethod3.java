/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84
 */
public class ExtractorCombinerMethod3 extends
        Reducer<RuleWritable, PairWritable, RuleWritable, PairWritable> {

    private final static DoubleWritable one = new DoubleWritable(1);

    protected void reduce(RuleWritable key, Iterable<PairWritable> values,
            Context context) throws java.io.IOException, InterruptedException {

        Map<RuleWritable, IntWritable> ruleCounts = new HashMap<RuleWritable, IntWritable>();
        // Map<RuleWritable, Integer> ruleCounts = new HashMap<RuleWritable,
        // Integer>();

        for (PairWritable targetCountPair: values) {
            // RuleWritable rw = new RuleWritable();
            // rw.setTarget(new Text(targetCountPair.first.getTarget()));
            // rw.setLeftHandSide(new Text("0"));
            RuleWritable target = new RuleWritable();
            target.setLeftHandSide(new Text("0"));
            target.setSource(new Text());
            target.setTarget(new Text(targetCountPair.first.getTarget()));
            IntWritable count = new IntWritable();
            count.set(targetCountPair.second.get());
            if (!ruleCounts.containsKey(target)) {
                ruleCounts.put(target, count);
                // ruleCounts.put(target, targetCountPair.second.get());
            }
            else {
                ruleCounts.put(target, new IntWritable(ruleCounts.get(target)
                        .get() + count.get()));
                // Integer sum = targetCountPair.second.get() +
                // ruleCounts.get(target);
                // ruleCounts.put(target, sum);
            }
        }

        for (RuleWritable rw: ruleCounts.keySet()) {
            int countRule = ruleCounts.get(rw).get();
            IntWritable newCount = new IntWritable(countRule);
            RuleWritable rwCopy = new RuleWritable();
            rwCopy.setSource(new Text(rw.getSource()));
            rwCopy.setTarget(new Text(rw.getTarget()));
            rwCopy.setLeftHandSide(new Text("0"));
            // context.write(key, new PairWritable(rw, ruleCounts.get(rw)));
            context.write(key, new PairWritable(rwCopy, newCount));
            // context.write(key, new PairWritable(key, new
            // IntWritable(ruleCounts.get(rw))));
        }

    }

}
