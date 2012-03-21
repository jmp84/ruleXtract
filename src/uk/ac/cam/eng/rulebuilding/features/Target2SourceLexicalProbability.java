/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SortedMapWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84
 */
public class Target2SourceLexicalProbability implements Feature {

    private final static String featureName =
            "target2source_lexical_probability";

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
        int featureIndex = conf.getInt(featureName, 0);
        res.put(featureIndex,
                ((DoubleWritable) mapReduceFeatures.get(featureIndex)).get());
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
        // if ascii rule, return the usual value. this can be different than
        // logMinSum in case the ascii constraint is actually part of the corpus
        if (r.getTargetWords().size() == 1 && r.getTargetWords().get(0) != 0) {
            return value(r, mapReduceFeatures, conf);
        }
        Map<Integer, Number> res = new HashMap<>();
        int featureIndex = conf.getInt(featureName, 0);
        res.put(featureIndex, 0);
        return res;
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#valueGlue(uk.ac.cam.eng.
     * extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public Map<Integer, Number> valueGlue(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf) {
        Map<Integer, Number> res = new HashMap<>();
        int featureIndex = conf.getInt(featureName, 0);
        res.put(featureIndex, 0);
        return res;
    }
}
