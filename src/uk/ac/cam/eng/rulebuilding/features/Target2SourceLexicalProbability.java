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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;

/**
 * @author jmp84
 */
public class Target2SourceLexicalProbability implements Feature {

    private final double minSum = 4.24e-18; // exp(-40)
    private final double logMinSum = -40;

    private Map<Integer, Map<Integer, Double>> model;

    private Set<Integer>
            getVocabulary(List<PairWritable3> rules, boolean source) {
        Set<Integer> res = new HashSet<Integer>();
        res.add(0); // null word
        for (PairWritable3 rule: rules) {
            Rule r = new Rule(rule.first);
            List<Integer> words =
                    source ? r.getSourceWords() : r.getTargetWords();
            for (int word: words) {
                res.add(word);
            }
        }
        return res;
    }

    public Target2SourceLexicalProbability(String modelFile,
            List<PairWritable3> rules) throws FileNotFoundException,
            IOException {
        Set<Integer> sourceVocabulary = getVocabulary(rules, true);
        Set<Integer> targetVocabulary = getVocabulary(rules, false);
        model = new HashMap<Integer, Map<Integer, Double>>();
        try (BufferedReader br =
                new BufferedReader(new InputStreamReader(new GZIPInputStream(
                        new FileInputStream(modelFile))))) {
            String line;
            int count = 1;
            while ((line = br.readLine()) != null) {
                if (count % 1000000 == 0) {
                    System.err.println("Processed " + count + " lines");
                }
                count++;
                String[] parts = line.split("\\s+");
                int sourceWord = Integer.parseInt(parts[1]);
                int targetWord = Integer.parseInt(parts[0]);
                double model1Probability = Double.parseDouble(parts[2]);
                if (!sourceVocabulary.contains(sourceWord)
                        || !targetVocabulary.contains(targetWord)) {
                    continue;
                }
                Map<Integer, Double> value = null;
                if (model.containsKey(targetWord)) {
                    value = model.get(targetWord);
                }
                else {
                    value = new HashMap<Integer, Double>();
                }
                value.put(sourceWord, model1Probability);
                model.put(targetWord, value);
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
        if (targetWords.size() > 1) {
            sourceWords.add(0);
        }
        for (Integer targetWord: targetWords) {
            double sum = 0;
            for (Integer sourceWord: sourceWords) {
                if (!model.containsKey(targetWord)) {
                    System.err
                            .println("Warning: model 1 missing target word: "
                                    + targetWord);
                }
                else if (!model.get(targetWord).containsKey(sourceWord)) {
                    System.err
                            .println("Warning: model 1 missing source word: "
                                    + sourceWord
                                    + " for target word: "
                                    + targetWord);
                }
                else {
                    sum += model.get(targetWord).get(sourceWord);
                }
            }
            if (sum > 0) {
                res *= sum;
            }
            else {
                res *= minSum;
            }
        }
        res /= Math.pow(sourceWords.size(), targetWords.size());
        // TODO could use the log in the computation
        return Math.log(res);
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
        if (r.getTargetWords().size() == 1 && r.getTargetWords().get(0) != 0) {
            return logMinSum;
        }
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
