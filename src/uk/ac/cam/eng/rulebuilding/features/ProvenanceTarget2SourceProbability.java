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
public class ProvenanceTarget2SourceProbability implements Feature {

    private final static String featureName =
            "provenance_target2source_probability";
    // TODO add this to the config
    private final static double defaultT2s = -7;

    private String provenance;

    public ProvenanceTarget2SourceProbability(String provenance) {
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
        double t2s = 0;
        if (mapReduceFeatures.containsKey(mapreduceFeatureIndex)) {
            t2s = ((DoubleWritable) mapReduceFeatures
                    .get(mapreduceFeatureIndex)).get();
        }
        int featureIndex = conf.getInt(featureName + "-" + provenance, 0);
        res.put(featureIndex, t2s == 0 ? defaultT2s : Math.log(t2s));
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
