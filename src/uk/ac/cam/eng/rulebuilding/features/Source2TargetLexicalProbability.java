/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84
 */
public class Source2TargetLexicalProbability implements Feature {

    private Map<Integer, Map<Integer, Double>> model;

    public Source2TargetLexicalProbability(String modelFile)
            throws FileNotFoundException, IOException {
        model = new HashMap<Integer, Map<Integer, Double>>();
        try (BufferedReader br =
                new BufferedReader(new InputStreamReader(new GZIPInputStream(
                        new FileInputStream(modelFile))))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                Map<Integer, Double> value = new HashMap<Integer, Double>();
                value.put(Integer.parseInt(parts[1]),
                        Double.parseDouble(parts[2]));
                model.put(Integer.parseInt(parts[0]), value);
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public double value(Rule r, ArrayWritable mapReduceFeatures) {
        double res = 1;
        List<Integer> sourceWords = r.getSourceWords();
        List<Integer> targetWords = r.getTargetWords();
        for (Integer sourceWord: sourceWords) {
            double sum = 0;
            for (Integer targetWord: targetWords) {
                sum += model.get(sourceWord).get(targetWord);
            }
            res *= sum;
        }
        return res;
    }

}
