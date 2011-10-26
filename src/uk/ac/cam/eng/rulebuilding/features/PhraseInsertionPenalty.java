/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This class represents the phrase insertion penalty feature,
 *         that is the feature that counts the number of rules used in
 *         translation
 */
public class PhraseInsertionPenalty implements Feature {

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .Rule)
     */
    @Override
    public double value(Rule r) {
        return 1;
    }

}
