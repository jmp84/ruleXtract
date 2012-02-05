/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 This class filters rules according to constraints in a config
 */
public class RuleFilter {

    private static class ValueComparator<K extends Comparable<K>, V extends Comparable<V>>
            implements Comparator<K> {

        private final Map<K, V> map;

        public ValueComparator(Map<K, V> map) {
            super();
            this.map = map;
        }

        public int compare(K key1, K key2) {
            V value1 = this.map.get(key1);
            V value2 = this.map.get(key2);
            int c = value2.compareTo(value1);
            if (c != 0) {
                return c;
            }
            // compare the keys because in case of ties, we want to keep the
            // most frequent source rules (smaller integers correspond to more
            // frequent words)
            return key1.compareTo(key2);
        }
    }

    private static <K extends Comparable<K>, V extends Comparable<V>> Map<K, V>
            sortMapByValue(
                    Map<K, V> unsortedMap) {
        SortedMap<K, V> sortedMap = new TreeMap<K, V>(
                new ValueComparator<K, V>(unsortedMap));
        sortedMap.putAll(unsortedMap);
        return sortedMap;
    }

    // TODO put the default in the code
    private double minSource2TargetPhrase;
    private double minTarget2SourcePhrase;
    private int maxSourcePhrase;
    private int maxTerminalLength;
    private int maxNonterminalLength;
    private int maxSourceElements;
    private double minSource2TargetRule;
    private double minTarget2SourceRule;
    // allowed patterns
    private Set<RulePattern> allowedPatterns;
    // skipped patterns: they count towards the maximum number of translation per source
    // threshold but are not included in the filtered rule file
    private Set<RulePattern> skipPatterns;
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
                else if (featureValue[0].equals("skip_pattern")) {
                	if (skipPatterns == null) {
                		skipPatterns = new HashSet<>();
                	}
                	skipPatterns.add(RulePattern.parsePattern(featureValue[1]));
                }
            }
        }
    }

    private ArrayWritable sortByCount(ArrayWritable listTargetAndProb) {
        Map<RuleWritable, Double> targetsAndCounts = new HashMap<>();
        Map<RuleWritable, Integer> indices = new HashMap<>();
        for (int i = 0; i < listTargetAndProb.get().length; i++) {
            targetsAndCounts
                    .put(((PairWritable3) listTargetAndProb.get()[i]).first,
                            ((DoubleWritable) ((PairWritable3) listTargetAndProb
                                    .get()[i]).second.get()[2]).get());
            indices.put(((PairWritable3) listTargetAndProb.get()[i]).first, i);
        }
        Map<RuleWritable, Double> sortedMap = sortMapByValue(targetsAndCounts);
        PairWritable3[] valueRes = new PairWritable3[sortedMap.size()];
        int i = 0;
        for (RuleWritable rw: sortedMap.keySet()) {
            valueRes[i] =
                    (PairWritable3) listTargetAndProb.get()[indices.get(rw)];
            i++;
        }
        return new ArrayWritable(PairWritable3.class, valueRes);
    }

    public List<PairWritable3> filter(RuleWritable source,
            ArrayWritable listTargetAndProb) {
        ArrayWritable listTargetAndProbSorted = sortByCount(listTargetAndProb);
        List<PairWritable3> res = new ArrayList<PairWritable3>();
        SidePattern sourcePattern = SidePattern.getSourcePattern(source);
        if (!sourcePattern.isPhrase()
                && !sourcePatternConstraints.containsKey(sourcePattern)) {
            return res;
        }
        int numberTranslations = 0;
        for (int i = 0; i < listTargetAndProbSorted.get().length; i++) {
            PairWritable3 targetAndProb =
                    (PairWritable3) listTargetAndProbSorted.get()[i];
            RulePattern rulePattern = RulePattern.getPattern(source,
                    targetAndProb.first);
            if (!sourcePattern.isPhrase()
                    && !allowedPatterns.contains(rulePattern)
                    && !skipPatterns.contains(rulePattern)) {
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
            // don't need to add !allowedPatterns.contains(rulePattern) because
            // this is checked above
            if (sourcePattern.isPhrase() || !skipPatterns.contains(rulePattern)) {
            	res.add(new PairWritable3(new RuleWritable(source,
            			targetAndProb.first), targetAndProb.second));
            }
            numberTranslations++;
        }
        return res;
    }
}
