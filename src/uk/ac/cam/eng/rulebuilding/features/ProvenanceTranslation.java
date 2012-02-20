/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84
 */
public class ProvenanceTranslation implements Feature {

    private String[] provenances;

    public ProvenanceTranslation(String[] provenances) {
        this.provenances = provenances;
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
        for (int i = 0; i < provenances.length; i++) {
            // 5 because the first mapreduce features are s2t, t2s, count, src
            // unaligned, trg unaligned
            // 3*i because the HFile contains source-to-target, target-to-source
            // and counts for each provenance
            res.add(Math.log(((DoubleWritable) mapReduceFeatures.get()[5 + 3 * i])
                    .get()));
            res.add(Math.log(((DoubleWritable) mapReduceFeatures.get()[5 + 3 * i + 1])
                    .get()));
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
        for (int i = 0; i < provenances.length; i++) {
            // 5 because the first mapreduce features are s2t, t2s, count, src
            // unaligned, trg unaligned
            res.add((double) 0);
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
        for (int i = 0; i < provenances.length; i++) {
            // 5 because the first mapreduce features are s2t, t2s, count, src
            // unaligned, trg unaligned
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
        return provenances.length;
    }
}
