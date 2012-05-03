/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84
 */
public class BinaryProvenance implements Feature {

    private final static String featureName = "binary_provenance";

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .datatypes.Rule, org.apache.hadoop.io.SortedMapWritable,
     * org.apache.hadoop.conf.Configuration)
     */
    @Override
    public Map<Integer, Number> value(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf) {
        Map<Integer, Number> res = new HashMap<>();
        int startMapReduceFeatureIndex =
                conf.getInt(featureName + "-mapreduce", 0);
        int startFeatureIndex = conf.getInt(featureName, 0);
        for (int i = 0; i < conf.getInt(featureName + "-nbfeats", 0); i++) {
            IntWritable mapReduceFeatureIndex =
                    new IntWritable(startMapReduceFeatureIndex + i);
            int featureIndex = startFeatureIndex + i;
            if (mapReduceFeatures.containsKey(mapReduceFeatureIndex)) {
                res.put(featureIndex, 1);
            }
        }
        return res;
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#valueAsciiOovDeletion(uk.
     * ac.cam.eng.extraction.datatypes.Rule,
     * org.apache.hadoop.io.SortedMapWritable,
     * org.apache.hadoop.conf.Configuration)
     */
    @Override
    public Map<Integer, Number> valueAsciiOovDeletion(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf) {
        return new HashMap<Integer, Number>();
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#valueGlue(uk.ac.cam.eng.
     * extraction.datatypes.Rule, org.apache.hadoop.io.SortedMapWritable,
     * org.apache.hadoop.conf.Configuration)
     */
    @Override
    public Map<Integer, Number> valueGlue(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf) {
        return new HashMap<Integer, Number>();
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#getNumberOfFeatures(org.apache
     * .hadoop.conf.Configuration)
     */
    @Override
    public int getNumberOfFeatures(Configuration conf) {
        return conf.getInt(featureName, 0);
    }
}
