/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This class represents the phrase insertion penalty feature,
 *         that is the feature that counts the number of rules used in
 *         translation
 */
public class RuleInsertionPenalty implements Feature {

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .Rule)
     */
    @Override
    public double value(Rule r, ArrayWritable mapReduceFeatures) {
        return (int) 1;
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#valueAsciiOovDeletion(uk.
     * ac.cam.eng.extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public double
            valueAsciiOovDeletion(Rule r, ArrayWritable mapReduceFeatures) {
        return 0;
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#valueGlue(uk.ac.cam.eng.
     * extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public double valueGlue(Rule r, ArrayWritable mapReduceFeatures) {
        if (r.isStartSentence() || r.isEndSentence()) {
            return 1;
        }
        return 0;
    }

}
