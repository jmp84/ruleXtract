/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

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

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Helper class for Source2TargetProbabilityReducer. Computes the
 *         source-to-target lexical probability.
 */
public class Source2TargetLexicalProbability {

    private final double minSum = 4.24e-18; // exp(-40)

    private Map<Integer, Map<Integer, Double>> model;

    private Set<Integer>
            getVocabulary(List<RuleWritable> rules, boolean source) {
        Set<Integer> res = new HashSet<Integer>();
        res.add(0); // null word
        for (RuleWritable rule: rules) {
            Rule r = new Rule(rule);
            List<Integer> words =
                    source ? r.getSourceWords() : r.getTargetWords();
            for (int word: words) {
                res.add(word);
            }
        }
        return res;
    }

    public Source2TargetLexicalProbability(String modelFile,
            List<RuleWritable> rules) throws FileNotFoundException, IOException {
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

    public double value(RuleWritable ruleWritable) {
        double lexprob = 1;
        Rule rule = new Rule(ruleWritable);
        List<Integer> sourceWords = rule.getSourceWords();
        List<Integer> targetWords = rule.getTargetWords();
        if (sourceWords.size() > 1) {
            targetWords.add(0);
        }
        for (Integer sourceWord: sourceWords) {
            double sum = 0;
            for (Integer targetWord: targetWords) {
                if (model.containsKey(sourceWord)
                        && model.get(sourceWord).containsKey(targetWord)) {
                    sum += model.get(sourceWord).get(targetWord);
                }
            }
            if (sum > 0) {
                lexprob *= sum;
            }
            else {
                lexprob *= minSum;
            }
        }
        lexprob /= Math.pow(targetWords.size(), sourceWords.size());
        // TODO could use the log in the computation
        return Math.log(lexprob);
    }
}
