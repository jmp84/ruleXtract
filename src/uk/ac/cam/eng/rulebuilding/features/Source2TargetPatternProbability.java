/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.rulebuilding.retrieval.RulePattern;

/**
 * @author jmp84 This class represents the source-to-target pattern translation
 *         feature.
 */
public class Source2TargetPatternProbability implements Feature {

    private Map<RulePattern, double[]> rulePatternAndFeatures;

    public Source2TargetPatternProbability(String rulePatternAndFeaturesFile)
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
    public double value(Rule r, ArrayWritable mapReduceFeatures) {
        RulePattern rp = RulePattern.getPattern(r);
        return Math.log(rulePatternAndFeatures.get(rp)[0]);
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
        return 0;
    }

}
