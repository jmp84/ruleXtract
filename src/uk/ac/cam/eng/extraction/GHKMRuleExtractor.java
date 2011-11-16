/**
 * 
 */

package uk.ac.cam.eng.extraction;

import java.util.ArrayList;
import java.util.List;

import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.AlignmentGraph;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import edu.berkeley.nlp.syntax.Tree;

/**
 * @author jmp84 Implementation of the GHKM algorithm (Galley et al. 2004)
 */
public class GHKMRuleExtractor {

    /**
     * Extract the rules following the GHKM algorithm. We use algorithm 2 of the
     * paper
     * 
     * @param sourceSentence
     * @param alignment
     * @param targetTree
     * @return
     */
    public List<Rule> extract(List<Integer> sourceSentence,
            Alignment alignment, Tree targetTree) {
        List<Rule> res = new ArrayList<Rule>();
        // get the alignment graph structure from the alignment, the sentence
        // and the target tree
        AlignmentGraph alignmentGraph =
                new AlignmentGraph(sourceSentence, alignment, targetTree);
        // compute the frontier set of the alignment graph
        alignmentGraph.computeFrontierSet();
        // "for each node of the frontier set, compute the minimal frontier
        // graph fragment rooted at that node" (Galley et al. 2004)
        Set<GraphFragment> minimalFrontierGraphFragments =
                alignmentGraph.getMinimalFrontierGraphFragments();
        for (GraphFragment graphFragment: minimalFrontierGraphFragments) {
            Rule rule = graphFragment.getRule();
            res.add(rule);
        }
        return res;
    }

}
