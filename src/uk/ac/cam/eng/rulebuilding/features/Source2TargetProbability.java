/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84
 */
public class Source2TargetProbability implements Feature {

    private final static String featureName = "source2target_probability";
    // TODO add this to the config
    private final static double defaultS2t = -4.7;

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public Map<Integer, Number> value(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf) {
        // TODO could use the log in the mapreduce job
        Map<Integer, Number> res = new HashMap<>();
        IntWritable mapreduceFeatureIndex =
                new IntWritable(conf.getInt(featureName + "-mapreduce", 0));
        double s2t = 0;
        // we need to check for the existence of the key in case there
        // are provenances outside the main table
        if (mapReduceFeatures.containsKey(mapreduceFeatureIndex)) {
            s2t = ((DoubleWritable) mapReduceFeatures
                    .get(mapreduceFeatureIndex)).get();
        }
        int featureIndex = conf.getInt(featureName, 0);
        res.put(featureIndex, s2t == 0 ? defaultS2t : Math.log(s2t));
        return res;
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#valueAsciiOovDeletion(uk.
     * ac.cam.eng.extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public Map<Integer, Number> valueAsciiOovDeletion(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf) {
        return new HashMap<>();
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#valueGlue(uk.ac.cam.eng.
     * extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public Map<Integer, Number> valueGlue(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf) {
        return new HashMap<>();
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#getNumberOfFeatures(org.apache
     * .hadoop.conf.Configuration)
     */
    @Override
    public int getNumberOfFeatures(Configuration conf) {
        return 1;
    }
}
