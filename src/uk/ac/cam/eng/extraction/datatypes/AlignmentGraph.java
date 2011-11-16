/**
 * 
 */

package uk.ac.cam.eng.extraction.datatypes;

import java.util.List;

import edu.berkeley.nlp.syntax.Tree;

/**
 * @author jmp84 This class represents an alignment graph as described in Galley
 *         et al. (2004)
 */
public class AlignmentGraph extends Tree {

    private List<Integer> sourceSentence;
    private Alignment alignment;

    /**
     * @param sourceSentence
     * @param alignment
     * @param tree
     */
    public AlignmentGraph(List<Integer> sourceSentence, Alignment alignment) {
        this.sourceSentence = sourceSentence;
        this.alignment = alignment;
    }

    private void getSpans() {

    }

    public void computeFrontierSet() {

    }

}
