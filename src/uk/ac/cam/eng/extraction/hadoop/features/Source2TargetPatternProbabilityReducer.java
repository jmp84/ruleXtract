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
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RulePatternWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84
 * 
 */
public class Source2TargetPatternProbabilityReducer extends
        Reducer<RuleWritable, IntWritable, RuleWritable, MapWritable> {

    /**
     * Starting index for this mapreduce feature. This is given by a config and
     * set in the setup method.
     */
    private static int featureStartIndex;
    /**
     * Name of the feature class. This is hard coded and used to retrieve
     * featureStartIndex from a config. TODO make all final
     */
    private final static String featureName =
            "source2target_pattern_probability";

    // static writables to avoid memory consumption
    private static MapWritable features = new MapWritable();
    private static DoubleWritable probability = new DoubleWritable();
    private static IntWritable featureIndex = new IntWritable();

    private int sourcePatternCount;
    private int patternCount;
    private Map<RulePatternWritable, Integer> patternsCount = new HashMap<>();

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
     * .Reducer.Context)
     */
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        featureStartIndex = conf.getInt(featureName, 0);
        featureIndex.set(featureStartIndex);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(RuleWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        if (key.isPattern() && ((RulePatternWritable) key).isTargetEmpty()) {
            sourcePatternCount = 0;
            for (IntWritable value : values) {
                sourcePatternCount += value.get();
            }
        } else if (key.isPattern()) {
            // patternsCount.put((RulePatternWritable) WritableUtils.clone(key,
            // context.getConfiguration()), 0);
            patternCount = 0;
            for (IntWritable value : values) {
                patternCount++;
            }
            patternsCount.put(
                    (RulePatternWritable) WritableUtils.clone(key,
                            context.getConfiguration()), patternCount);
        } else {
            RulePatternWritable pattern = new RulePatternWritable(key);
            probability.set((double) patternsCount.get(pattern)
                    / sourcePatternCount);
            features.put(featureIndex, features);
            context.write(key, features);
        }
    }
}
