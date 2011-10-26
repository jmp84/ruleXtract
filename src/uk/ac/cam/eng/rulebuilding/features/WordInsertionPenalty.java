/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This class represent the word insertion penalty feature, that
 *         is the number of terminals in the target side of a rule
 */
public class WordInsertionPenalty implements Feature {

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .Rule)
     */
    @Override
    public double value(Rule r) {
        return r.nbTargetWords();
    }

}
