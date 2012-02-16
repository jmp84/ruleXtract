/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.util.List;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84
 */
public class ProvenanceLexical implements Feature {

    /**
     * List of source-to-target model 1 files
     */
    private String[] source2targetModelFiles;

    public ProvenanceLexical(String[] source2targetModelFiles) {
        this.source2targetModelFiles = source2targetModelFiles;
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public List<Double> value(Rule r, ArrayWritable mapReduceFeatures) {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#valueGlue(uk.ac.cam.eng.
     * extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public List<Double> valueGlue(Rule r, ArrayWritable mapReduceFeatures) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#getNumberOfFeatures()
     */
    @Override
    public int getNumberOfFeatures() {
        // TODO Auto-generated method stub
        return 0;
    }

}
