/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This class represents the glue rule feature, that is the number
 *         of times the rule S-->SX,SX is found
 */
public class GlueRule implements Feature {

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .Rule)
     */
    @Override
    public List<Double> value(Rule r, ArrayWritable mapReduceFeatures) {
        // TODO the cast to int is just for the printing. necessary ?
        // return (int) (r.isConcatenatingGlue() ? 1 : 0);
        List<Double> res = new ArrayList<>();
        if (r.isConcatenatingGlue()) {
            res.add((double) 1);
        }
        else {
            res.add((double) 0);
        }
        return res;
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#valueAsciiOovDeletion(uk.
     * ac.cam.eng.extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public List<Double>
            valueAsciiOovDeletion(Rule r, ArrayWritable mapReduceFeatures) {
        List<Double> res = new ArrayList<>();
        res.add((double) 0);
        return res;
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#valueGlue(uk.ac.cam.eng.
     * extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public List<Double> valueGlue(Rule r, ArrayWritable mapReduceFeatures) {
        List<Double> res = new ArrayList<>();
        if (r.isConcatenatingGlue()) {
            res.add((double) 1);
        }
        else {
            res.add((double) 0);
        }
        return res;
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#getNumberOfFeatures()
     */
    @Override
    public int getNumberOfFeatures() {
        return 1;
    }
}
