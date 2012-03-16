/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Reducer to compute binary provenance feature. Simply merge the
 *         binary features into a map and taking the offset into account.
 */
public class BinaryProvenanceReducer extends
        Reducer<RuleWritable, RuleInfoWritable, RuleWritable, MapWritable> {

    /**
     * Starting index for this mapreduce feature. This is given by a config and
     * set in the setup method.
     */
    private static int featureStartIndex;
    /**
     * Name of the feature class. This is hard coded and used to retrieve
     * featureStartIndex from a config.
     */
    private static String featureName = "binaryProvenance";

    // static writables to avoid memory consumption
    private static MapWritable features = new MapWritable();
    private static IntWritable one = new IntWritable(1);

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
        // TODO add a check here
        featureStartIndex = conf.getInt(featureName, 0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(RuleWritable key, Iterable<RuleInfoWritable> values,
            Context context) throws IOException, InterruptedException {
        for (RuleInfoWritable ruleInfoWritable : values) {
            for (Writable provenance : ruleInfoWritable.getBinaryProvenance()
                    .keySet()) {
                IntWritable featureIndex =
                        new IntWritable(featureStartIndex
                                + ((IntWritable) provenance).get());
                features.put(featureIndex, one);
            }
        }
        context.write(key, features);
    }
}
