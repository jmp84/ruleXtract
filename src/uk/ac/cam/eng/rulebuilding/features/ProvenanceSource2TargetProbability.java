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
public class ProvenanceSource2TargetProbability implements Feature {

    private final static String featureName =
            "provenance_source2target_probability";
    // TODO add this to the config
    private final static double defaultS2t = -4.7;

    private String provenance;

    public ProvenanceSource2TargetProbability(String provenance) {
        this.provenance = provenance;
    }

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
        IntWritable mapreduceFeatureIndex =
                new IntWritable(conf.getInt(featureName + "-" + provenance
                        + "-mapreduce", 0));
        double s2t = 0;
        if (mapReduceFeatures.containsKey(mapreduceFeatureIndex)) {
            s2t = ((DoubleWritable) mapReduceFeatures
                    .get(mapreduceFeatureIndex)).get();
        }
        int featureIndex = conf.getInt(featureName + "-" + provenance, 0);
        res.put(featureIndex, s2t == 0 ? defaultS2t : Math.log(s2t));
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
        return new HashMap<>();
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
