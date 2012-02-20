/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;

/**
 * @author jmp84
 */
public class ProvenanceLexical implements Feature {

    /**
     * List of source-to-target and target-to-source models 1
     */
    private List<Source2TargetLexicalProbability> source2targetModels;
    private List<Target2SourceLexicalProbability> target2sourceModels;

    public ProvenanceLexical(String[] source2targetModelFiles,
            String[] target2sourceModelFiles, List<PairWritable3> rules)
            throws FileNotFoundException, IOException {
        if (source2targetModelFiles.length != target2sourceModelFiles.length) {
            System.err.println("ERROR: different number of source-to-target " +
                    "and target-to-source lexical models: ");
            System.exit(1);
        }
        for (int i = 0; i < source2targetModelFiles.length; i++) {
            source2targetModels.add(new Source2TargetLexicalProbability(
                    source2targetModelFiles[i], rules));
            target2sourceModels.add(new Target2SourceLexicalProbability(
                    target2sourceModelFiles[i], rules));
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public List<Double> value(Rule r, ArrayWritable mapReduceFeatures) {
        List<Double> res = new ArrayList<>();
        for (int i = 0; i < source2targetModels.size(); i++) {
            res.addAll(source2targetModels.get(i).value(r, mapReduceFeatures));
            res.addAll(target2sourceModels.get(i).value(r, mapReduceFeatures));
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
    public List<Double> valueAsciiOovDeletion(Rule r,
            ArrayWritable mapReduceFeatures) {
        List<Double> res = new ArrayList<>();
        for (int i = 0; i < source2targetModels.size(); i++) {
            res.addAll(source2targetModels.get(i).valueAsciiOovDeletion(r,
                    mapReduceFeatures));
            res.addAll(target2sourceModels.get(i).valueAsciiOovDeletion(r,
                    mapReduceFeatures));
        }
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
        for (int i = 0; i < source2targetModels.size(); i++) {
            res.addAll(source2targetModels.get(i).valueGlue(r,
                    mapReduceFeatures));
            res.addAll(target2sourceModels.get(i).valueGlue(r,
                    mapReduceFeatures));
        }
        return res;
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#getNumberOfFeatures()
     */
    @Override
    public int getNumberOfFeatures() {
        return source2targetModels.size() + target2sourceModels.size();
    }
}
