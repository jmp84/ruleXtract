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
public class Source2TargetLexicalProbability implements Feature {

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

    public Source2TargetLexicalProbability(String modelFile,
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
                int sourceWord = Integer.parseInt(parts[0]);
                int targetWord = Integer.parseInt(parts[1]);
                double model1Probability = Double.parseDouble(parts[2]);
                if (!sourceVocabulary.contains(sourceWord)
                        || !targetVocabulary.contains(targetWord)) {
                    continue;
                }
                Map<Integer, Double> value = null;
                if (model.containsKey(sourceWord)) {
                    value = model.get(sourceWord);
                }
                else {
                    value = new HashMap<Integer, Double>();
                }
                value.put(targetWord, model1Probability);
                model.put(sourceWord, value);
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
        if (sourceWords.size() > 1) {
            targetWords.add(0);
        }
        for (Integer sourceWord: sourceWords) {
            double sum = 0;
            for (Integer targetWord: targetWords) {
                if (!model.containsKey(sourceWord)) {
                    System.err.println("Warning: model 1 missing source word: "
                            + sourceWord);
                }
                else if (!model.get(sourceWord).containsKey(targetWord)) {
                    System.err.println("Warning: model 1 missing target word: "
                            + targetWord + " for source word: " + sourceWord);
                }
                else {
                    sum += model.get(sourceWord).get(targetWord);
                }
            }
            if (sum > 0) {
                res *= sum;
            }
            else {
                res *= minSum;
            }
        }
        res /= Math.pow(targetWords.size(), sourceWords.size());
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
        // if ascii rule, return logMinSum
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
