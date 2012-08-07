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
public class UnalignedTargetWords implements Feature {

    private final static String featureName = "unaligned_target_words";

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public Map<Integer, Number> value(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf) {
        Map<Integer, Number> res = new HashMap<>();
        // the mapreduce feature unaligned_words has 2 features, the first is
        // unaligned_source_words and the second is unaligned_target_words
        IntWritable mapreduceFeatureIndex =
                new IntWritable(conf.getInt("unaligned_words-mapreduce", 0) + 1);
        int featureIndex = conf.getInt(featureName, 0);
        res.put(featureIndex,
                ((DoubleWritable) mapReduceFeatures.get(mapreduceFeatureIndex))
                        .get());
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
