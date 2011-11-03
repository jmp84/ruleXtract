/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 This class filters rules according to constraints in a config
 */
public class RuleFilter {

    // TODO put the default in the code
    private double minSource2TargetPhrase;
    private double minTarget2SourcePhrase;
    private int maxSourcePhrase;
    private int maxTerminalLength;
    private int maxNonterminalLength;
    private int maxSourceElements;
    private double minSource2TargetRule;
    private double minTarget2SourceRule;
    private Set<RulePattern> allowedPatterns;
    private Map<SidePattern, Map<String, Double>> sourcePatternConstraints;

    public void loadConfig(String configFile) throws FileNotFoundException,
            IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String line;
            String[] parts;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                if (line.isEmpty()) {
                    continue;
                }
                parts = line.split("\\s+");
                if (parts.length == 0) {
                    System.err.println("Malformed config file");
                    System.exit(1);
                }
                String[] featureValue = parts[0].split("=", 2);
                if (featureValue[0].equals("max_source_phrase")) {
                    maxSourcePhrase = Integer.parseInt(featureValue[1]);
                }
                else if (featureValue[0].equals("min_source2target_phrase")) {
                    minSource2TargetPhrase = Double
                            .parseDouble(featureValue[1]);
                }
                else if (featureValue[0].equals("min_target2source_phrase")) {
                    minTarget2SourcePhrase = Double
                            .parseDouble(featureValue[1]);
                }
                else if (featureValue[0].equals("max_terminal_length")) {
                    maxTerminalLength = Integer.parseInt(featureValue[1]);
                }
                else if (featureValue[0].equals("max_nonterminal_length")) {
                    maxNonterminalLength = Integer.parseInt(featureValue[1]);
                }
                else if (featureValue[0].equals("max_source_elements")) {
                    maxSourceElements = Integer.parseInt(featureValue[1]);
                }
                else if (featureValue[0].equals("min_source2target_rule")) {
                    minSource2TargetRule = Double.parseDouble(featureValue[1]);
                }
                else if (featureValue[0].equals("min_target2source_phrase")) {
                    minTarget2SourceRule = Double.parseDouble(featureValue[1]);
                }
                else if (featureValue[0].equals("allowed_source_pattern")) {
                    if (sourcePatternConstraints == null) {
                        sourcePatternConstraints =
                                new HashMap<SidePattern, Map<String, Double>>();
                    }
                    Map<String, Double> constraints =
                            new HashMap<String, Double>();
                    for (int i = 1; i < parts.length; i++) {
                        String[] constraintValue = parts[i].split("=", 2);
                        constraints.put(constraintValue[0],
                                Double.parseDouble(constraintValue[1]));
                    }
                    sourcePatternConstraints.put(
                            SidePattern.parsePattern(featureValue[1]),
                            constraints);
                }
                else if (featureValue[0].equals("allowed_pattern")) {
                    if (allowedPatterns == null) {
                        allowedPatterns = new HashSet<RulePattern>();
                    }
                    allowedPatterns.add(RulePattern
                            .parsePattern(featureValue[1]));
                }
            }
        }
    }

    public List<PairWritable3> filter(RuleWritable source,
            ArrayWritable listTargetAndProb) {
        List<PairWritable3> res = new ArrayList<PairWritable3>();
        SidePattern sourcePattern = SidePattern.getSourcePattern(source);
        if (!sourcePattern.isPhrase()
                && !sourcePatternConstraints.containsKey(sourcePattern)) {
            return res;
        }
        int numberTranslations = 0;
        for (int i = 0; i < listTargetAndProb.get().length; i++) {
            PairWritable3 targetAndProb = (PairWritable3) listTargetAndProb
                    .get()[i];
            RulePattern rulePattern = RulePattern.getPattern(source,
                    targetAndProb.first);
            if (!sourcePattern.isPhrase()
                    && !allowedPatterns.contains(rulePattern)) {
                continue;
            }
            if (sourcePattern.isPhrase()) {
                // TODO replace hardcoded feature indices
                // source-to-target threshold
                if (((DoubleWritable) targetAndProb.second.get()[0]).get() <= minSource2TargetPhrase) {
                    // break;
                    // TODO currently targets are sorted in alphabetical order.
                    // We need to sort them by count (or equivalently by
                    // source-to-target probability), so we can use a break
                    // instead of a continue. In the current pipeline, the
                    // maximum number of translation per source relies on this
                    // ordering
                    continue;
                }
                // target-to-source threshold
                if (((DoubleWritable) targetAndProb.second.get()[1]).get() <= minTarget2SourcePhrase) {
                    continue;
                }
            }
            else {
                // source-to-target threshold
                if (((DoubleWritable) targetAndProb.second.get()[0]).get() <= minSource2TargetRule) {
                    // break;
                    continue;
                }
                // target-to-source threshold
                if (((DoubleWritable) targetAndProb.second.get()[1]).get() <= minTarget2SourceRule) {
                    continue;
                }
                // minimum number of occurrence threshold
                if (sourcePatternConstraints.get(sourcePattern).containsKey(
                        "nocc")) {
                    if (((DoubleWritable) targetAndProb.second.get()[2]).get() < sourcePatternConstraints
                            .get(sourcePattern).get("nocc")) {
                        continue;
                    }
                }
                if (sourcePatternConstraints.get(sourcePattern).get("ntrans") <= numberTranslations) {
                    break;
                }
            }
            res.add(new PairWritable3(new RuleWritable(source,
                    targetAndProb.first), targetAndProb.second));
            numberTranslations++;
        }
        return res;
    }
}
