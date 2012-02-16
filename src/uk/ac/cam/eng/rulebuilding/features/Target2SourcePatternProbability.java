/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.rulebuilding.retrieval.RulePattern;

/**
 * @author jmp84 This class represents the target-to-source pattern translation
 *         feature.
 */
public class Target2SourcePatternProbability implements Feature {

    private Map<RulePattern, double[]> rulePatternAndFeatures;

    public Target2SourcePatternProbability(String rulePatternAndFeaturesFile)
            throws FileNotFoundException, IOException {
        try (BufferedReader br =
                new BufferedReader(new FileReader(rulePatternAndFeaturesFile))) {
            rulePatternAndFeatures = new HashMap<>();
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length != 5) {
                    System.err.println(
                            "Malformed pattern and features: " + line);
                    System.exit(1);
                }
                RulePattern rp = RulePattern.parsePattern(parts[0], parts[1]);
                double[] features = new double[3];
                features[0] = Double.parseDouble(parts[2]);
                features[1] = Double.parseDouble(parts[3]);
                features[2] = Double.parseDouble(parts[4]);
                if (rulePatternAndFeatures.containsKey(rp)) {
                    System.err.println(
                            "ERROR: Rule pattern not unique: " + line);
                    System.exit(1);
                }
                rulePatternAndFeatures.put(rp, features);
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .Rule)
     */
    @Override
    public List<Double> value(Rule r, ArrayWritable mapReduceFeatures) {
        List<Double> res = new ArrayList<>();
        RulePattern rp = RulePattern.getPattern(r);
        res.add(Math.log(rulePatternAndFeatures.get(rp)[1]));
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
        res.add((double) 0);
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
