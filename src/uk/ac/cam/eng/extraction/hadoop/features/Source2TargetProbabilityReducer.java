/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Reducer to compute source-to-target probability
 */
public class Source2TargetProbabilityReducer
        extends
        Reducer<RuleWritable, PairWritable, RuleWritable, MapWritable> {

    /**
     * Starting index for this mapreduce feature. This is given by a config and
     * set in the setup method.
     */
    private static int featureStartIndex;
    /**
     * Name of the feature class. This is hard coded and used to retrieve
     * featureStartIndex from a config.
     */
    private static String featureName = "source2target_probability";

    // static writables to avoid memory consumption
    private static MapWritable features = new MapWritable();
    private static DoubleWritable probability = new DoubleWritable();
    private static IntWritable count = new IntWritable();

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
     * .Reducer.Context)
     */
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        featureStartIndex = conf.getInt(featureName, 0);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(RuleWritable key, Iterable<PairWritable> values,
            Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // first loop through the targets and gather counts
        double marginalCount = 0;
        // use HashMap because we don't need to have the rules sorted
        Map<RuleWritable, Integer> ruleCounts =
                new HashMap<RuleWritable, Integer>();
        for (PairWritable targetAndCount: values) {
            marginalCount += targetAndCount.second.get();
            RuleWritable rw = new RuleWritable(key, targetAndCount.first);
            if (!ruleCounts.containsKey(rw)) {
                ruleCounts.put(rw, targetAndCount.second.get());
            }
            else {
                ruleCounts.put(rw,
                        ruleCounts.get(rw) + targetAndCount.second.get());
            }
        }
        // do a second pass for normalization
        for (RuleWritable rw: ruleCounts.keySet()) {
            count.set(ruleCounts.get(rw));
            probability.set(count.get() / marginalCount);
            IntWritable featureIndex = new IntWritable(featureStartIndex);
            features.put(featureIndex, probability);
            featureIndex = new IntWritable(featureStartIndex + 1);
            features.put(featureIndex, count);
            context.write(rw, features);
        }
    }
}
