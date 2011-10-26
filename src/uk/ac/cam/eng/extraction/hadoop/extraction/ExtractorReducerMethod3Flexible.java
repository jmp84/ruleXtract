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

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84
 */
public class ExtractorReducerMethod3Flexible
        extends
        Reducer<RuleWritable, PairWritable<RuleWritable, IntWritable>, RuleWritable, DoubleWritable> {

    // TODO remove object creation as much as possible

    private final static DoubleWritable one = new DoubleWritable(1);

    protected void reduce(RuleWritable key,
            Iterable<PairWritable<RuleWritable, IntWritable>> values,
            Context context) throws java.io.IOException, InterruptedException {

        // debug code
        /*
         * Map<String, Double> ruleCounts = new TreeMap<String, Double>(); for
         * (PairWritable targetCountPair: values) { RuleWritable rw = new
         * RuleWritable(); rw.setLeftHandSide(new Text("0"));
         * rw.setSource(key.getSource());
         * rw.setTarget(targetCountPair.first.getTarget());
         * //context.write(rw,one); ruleCounts.put(rw.toString(), 0d); } for
         * (String r: ruleCounts.keySet()) { String[] parts = r.split("\\s+");
         * RuleWritable rw = new RuleWritable(); rw.setLeftHandSide(new
         * Text("0")); rw.setSource(new Text(parts[1])); rw.setTarget(new
         * Text(parts[2])); context.write(rw, one); }
         */
        /*
         * Map<RuleWritable, DoubleWritable> ruleCounts = new
         * HashMap<RuleWritable, DoubleWritable>(); for (PairWritable
         * targetCountPair: values) { RuleWritable rw = new RuleWritable(); //
         * TODO replace this by deep copy rw.setLeftHandSide(new Text("0"));
         * rw.setSource(new Text(key.getSource())); rw.setTarget(new
         * Text(targetCountPair.first.getTarget())); //context.write(rw,one);
         * //if (!ruleCounts.containsKey(rw)) { ruleCounts.put(rw, one); //} }
         * for (RuleWritable rw: ruleCounts.keySet()) { context.write(rw, one);
         * //System.err.println(rw); } //
         */

        // /*
        double marginalCount = 0;
        // use a TreeMap to get the rules sorted
        Map<RuleWritable, IntWritable> ruleCounts = new HashMap<RuleWritable, IntWritable>();

        for (PairWritable<RuleWritable, IntWritable> targetCountPair: values) {
            marginalCount += targetCountPair.second.get();
            RuleWritable rw = new RuleWritable();
            rw.setSource(new Text(key.getSource()));
            rw.setTarget(new Text(targetCountPair.first.getTarget()));
            rw.setLeftHandSide(new Text("0"));
            if (!ruleCounts.containsKey(rw)) {
                ruleCounts.put(rw, targetCountPair.second);
            }
            else {
                ruleCounts.put(rw, new IntWritable(ruleCounts.get(rw).get()
                        + targetCountPair.second.get()));
            }
        }

        // do a second pass for normalization

        for (RuleWritable rw: ruleCounts.keySet()) {
            double countRule = ruleCounts.get(rw).get();
            DoubleWritable sourceToTargetProb = new DoubleWritable(countRule
                    / marginalCount);
            context.write(rw, sourceToTargetProb);
        }
        // */
    }

}
