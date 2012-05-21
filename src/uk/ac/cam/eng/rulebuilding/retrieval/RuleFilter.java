/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

// TODO remove hard coded indices

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.features.MapReduceFeature;
import uk.ac.cam.eng.extraction.hadoop.features.MapReduceFeatureCreator;

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
            sortMapByValue(Map<K, V> unsortedMap) {
        SortedMap<K, V> sortedMap =
                new TreeMap<K, V>(new ValueComparator<K, V>(unsortedMap));
        sortedMap.putAll(unsortedMap);
        return sortedMap;
    }

    // TODO put the default in the code
    private double minSource2TargetPhrase;
    private double minTarget2SourcePhrase;
    private double minSource2TargetRule;
    private double minTarget2SourceRule;
    // allowed patterns
    private Set<RulePattern> allowedPatterns;
    // skipped patterns: they count towards the maximum number of translation
    // per source
    // threshold but are not included in the filtered rule file
    private Set<RulePattern> skipPatterns;
    private Map<SidePattern, Map<String, Double>> sourcePatternConstraints;
    // decides whether to keep all the rules that fall within the number
    // of translations per source threshold in case of a tie
    private boolean keepTiedRules = true;
    // when using provenance features, we can either keep the rules coming
    // from the main table and add features corresponding to the provenance
    // tables (default) or keep the union of the rules coming from the main
    // and provenance tables
    private boolean provenanceUnion = false;
    private Configuration conf;

    public RuleFilter(Configuration conf) {
        // TODO this part is repeated in several places. keep it in one place
        String mapreduceFeaturesString = conf.get("mapreduce_features");
        if (mapreduceFeaturesString == null) {
            System.err.println("Missing property " +
                    "'mapreduce_features' in the config");
            System.exit(1);
        }
        String[] mapreduceFeatures = mapreduceFeaturesString.split(",");
        MapReduceFeatureCreator featureCreator =
                new MapReduceFeatureCreator(conf);
        int featureIndex = 0, nextFeatureIndex = 0;
        for (String mapreduceFeature: mapreduceFeatures) {
            if (mapreduceFeature.equals(
                    "provenance_source2target_lexical_probability")
                    || mapreduceFeature
                            .equals("provenance_target2source_lexical_probability")
                    || mapreduceFeature
                            .equals("provenance_source2target_probability")
                    || mapreduceFeature
                            .equals("provenance_target2source_probability")) {
                for (String prov: conf.get("provenance").split(",")) {
                    featureIndex = nextFeatureIndex;
                    MapReduceFeature featureJob =
                            featureCreator.getFeatureJob(mapreduceFeature + "-"
                                    + prov);
                    nextFeatureIndex += featureJob.getNumberOfFeatures(conf);
                    conf.setInt(mapreduceFeature + "-" + prov + "-mapreduce",
                            featureIndex);
                }
            }
            else {
                featureIndex = nextFeatureIndex;
                MapReduceFeature featureJob =
                        featureCreator.getFeatureJob(mapreduceFeature);
                nextFeatureIndex += featureJob.getNumberOfFeatures(conf);
                // add "-mapreduce" to avoid name clashing
                conf.setInt(mapreduceFeature + "-mapreduce", featureIndex);
            }
        }
        this.conf = conf;
    }

    // TODO use Properties instead
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
                if (featureValue[0].equals("min_source2target_phrase")) {
                    minSource2TargetPhrase =
                            Double.parseDouble(featureValue[1]);
                }
                else if (featureValue[0].equals("min_target2source_phrase")) {
                    minTarget2SourcePhrase =
                            Double.parseDouble(featureValue[1]);
                }
                else if (featureValue[0].equals("min_source2target_rule")) {
                    minSource2TargetRule = Double.parseDouble(featureValue[1]);
                }
                else if (featureValue[0].equals("min_target2source_rule")) {
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
                else if (featureValue[0].equals("keep_tied_rules")) {
                    keepTiedRules = Boolean.parseBoolean(featureValue[1]);
                }
                else if (featureValue[0].equals("provenance_union")) {
                    provenanceUnion = Boolean.parseBoolean(featureValue[1]);
                }
            }
        }
    }

    // TODO countIndex int or IntWritable ?
    private ArrayWritable sortByCount(ArrayWritable listTargetAndProb,
            int countIndex) {
        Map<RuleWritable, Integer> targetsAndCounts = new HashMap<>();
        Map<RuleWritable, Integer> indices = new HashMap<>();
        IntWritable countIndexWritable = new IntWritable(countIndex);
        for (int i = 0; i < listTargetAndProb.get().length; i++) {
            // need to check existence of the key because sparse map
            if (((GeneralPairWritable3) listTargetAndProb.get()[i]).getSecond()
                    .containsKey(countIndexWritable)) {
                targetsAndCounts
                        .put(((GeneralPairWritable3) listTargetAndProb
                                .get()[i]).getFirst(),
                                ((IntWritable) ((GeneralPairWritable3) listTargetAndProb
                                        .get()[i]).getSecond().get(
                                        countIndexWritable))
                                        .get());
                indices.put(((GeneralPairWritable3) listTargetAndProb.get()[i])
                        .getFirst(), i);
            }
        }
        Map<RuleWritable, Integer> sortedMap = sortMapByValue(targetsAndCounts);
        GeneralPairWritable3[] valueRes =
                new GeneralPairWritable3[sortedMap.size()];
        int i = 0;
        for (RuleWritable rw: sortedMap.keySet()) {
            valueRes[i] =
                    (GeneralPairWritable3) listTargetAndProb.get()[indices
                            .get(rw)];
            i++;
        }
        return new ArrayWritable(GeneralPairWritable3.class, valueRes);
    }

    public List<GeneralPairWritable3> filter(RuleWritable source,
            ArrayWritable listTargetAndProb, String provenance) {
        int s2tIndex =
                provenance.equals("") ? conf.getInt(
                        "source2target_probability-mapreduce", 0) : conf
                        .getInt("provenance_source2target_probability-"
                                + provenance + "-mapreduce", 0);
        IntWritable source2targetProbabilityIndex = new IntWritable(s2tIndex);
        int t2sIndex =
                provenance.equals("") ? conf.getInt(
                        "target2source_probability-mapreduce", 0) : conf
                        .getInt("provenance_target2source_probability-"
                                + provenance + "-mapreduce", 0);
        IntWritable target2sourceProbabilityIndex = new IntWritable(t2sIndex);
        IntWritable countIndex = new IntWritable(s2tIndex + 1);
        ArrayWritable listTargetAndProbSorted =
                sortByCount(listTargetAndProb, countIndex.get());
        List<GeneralPairWritable3> res = new ArrayList<GeneralPairWritable3>();
        SidePattern sourcePattern = SidePattern.getSourcePattern(source);
        if (!sourcePattern.isPhrase()
                && !sourcePatternConstraints.containsKey(sourcePattern)) {
            return res;
        }
        int numberTranslations = 0;
        int numberTranslationsMonotone = 0; // case with more than 1 NT
        int numberTranslationsInvert = 0;
        // used in case of ties in number of occurrences
        // we separate ties for rules with two nonterminals that
        // are monotone or inverting
        int previousNumberOfOccurrences = -1;
        int previousNumberOfOccurrences2NTmonotone = -1;
        int previousNumberOfOccurrences2NTinvert = -1;
        boolean tie = false;
        boolean twoNTmonotoneTie = false;
        boolean twoNTinvertTie = false;
        for (int i = 0; i < listTargetAndProbSorted.get().length; i++) {
            GeneralPairWritable3 targetAndProb =
                    (GeneralPairWritable3) listTargetAndProbSorted.get()[i];
            double source2targetProbability =
                    ((DoubleWritable) targetAndProb.getSecond().get(
                            source2targetProbabilityIndex)).get();
            double target2sourceProbability =
                    ((DoubleWritable) targetAndProb.getSecond().get(
                            target2sourceProbabilityIndex)).get();
            int numberOfOccurrences =
                    ((IntWritable) targetAndProb.getSecond().get(countIndex))
                            .get();
            if (numberOfOccurrences == previousNumberOfOccurrences) {
                tie = true;
            }
            else {
                tie = false;
            }
            previousNumberOfOccurrences = numberOfOccurrences;
            RulePattern rulePattern =
                    RulePattern.getPattern(source, targetAndProb.getFirst());
            if (sourcePattern.hasMoreThan1NT()) {
                if (rulePattern.isSwappingNT()) {
                    if (numberOfOccurrences == previousNumberOfOccurrences2NTinvert) {
                        twoNTinvertTie = true;
                    }
                    else {
                        twoNTinvertTie = false;
                    }
                    previousNumberOfOccurrences2NTinvert = numberOfOccurrences;
                }
                else {
                    if (numberOfOccurrences == previousNumberOfOccurrences2NTmonotone) {
                        twoNTmonotoneTie = true;
                    }
                    else {
                        twoNTmonotoneTie = false;
                    }
                    previousNumberOfOccurrences2NTmonotone =
                            numberOfOccurrences;
                }
            }
            if (!sourcePattern.isPhrase()
                    && !allowedPatterns.contains(rulePattern)
                    && (skipPatterns == null || !skipPatterns
                            .contains(rulePattern))) {
                continue;
            }
            if (sourcePattern.isPhrase()) {
                // source-to-target threshold
                if (source2targetProbability <= minSource2TargetPhrase) {
                    break;
                }
                // target-to-source threshold
                if (target2sourceProbability <= minTarget2SourcePhrase) {
                    continue;
                }
            }
            else {
                // source-to-target threshold
                if (source2targetProbability <= minSource2TargetRule) {
                    break;
                }
                // target-to-source threshold
                if (target2sourceProbability <= minTarget2SourceRule) {
                    continue;
                }
                // minimum number of occurrence threshold
                if (sourcePatternConstraints.get(sourcePattern).containsKey(
                        "nocc")) {
                    if (numberOfOccurrences < sourcePatternConstraints.get(
                            sourcePattern).get("nocc")) {
                        break;
                    }
                }
                // number of translations per source threshold
                // in case of ties we either keep or don't keep the ties
                // depending on the config
                if (sourcePattern.hasMoreThan1NT()) {
                    if (sourcePatternConstraints.get(sourcePattern).get(
                            "ntrans") <= numberTranslationsMonotone
                            && sourcePatternConstraints.get(sourcePattern).get(
                                    "ntrans") <= numberTranslationsInvert
                            && (!keepTiedRules || (keepTiedRules
                                    && !twoNTmonotoneTie && !twoNTinvertTie))) {
                        break;
                    }
                }
                else if (sourcePatternConstraints.get(sourcePattern).get(
                        "ntrans") <= numberTranslations
                        && (!keepTiedRules || (keepTiedRules && !tie))) {
                    break;
                }
            }
            // don't need to add !allowedPatterns.contains(rulePattern) because
            // this is checked above
            if (sourcePattern.isPhrase()) {
                res.add(new GeneralPairWritable3(new RuleWritable(source,
                        targetAndProb.getFirst()), targetAndProb.getSecond()));
            }
            else if (sourcePattern.hasMoreThan1NT()) {
                if (skipPatterns == null || !skipPatterns.contains(rulePattern)) {
                    if (rulePattern.isSwappingNT()) {
                        if (sourcePatternConstraints.get(sourcePattern).get(
                                "ntrans") > numberTranslationsInvert
                                || (keepTiedRules && twoNTinvertTie)) {
                            res.add(new GeneralPairWritable3(new RuleWritable(
                                    source,
                                    targetAndProb.getFirst()), targetAndProb
                                    .getSecond()));
                        }
                    }
                    else {
                        if (sourcePatternConstraints.get(sourcePattern).get(
                                "ntrans") > numberTranslationsMonotone
                                || (keepTiedRules && twoNTmonotoneTie)) {
                            res.add(new GeneralPairWritable3(new RuleWritable(
                                    source,
                                    targetAndProb.getFirst()), targetAndProb
                                    .getSecond()));
                        }
                    }
                }
            }
            else if (skipPatterns == null
                    || !skipPatterns.contains(rulePattern)) {
                res.add(new GeneralPairWritable3(new RuleWritable(source,
                        targetAndProb.getFirst()), targetAndProb.getSecond()));
            }
            if (sourcePattern.hasMoreThan1NT()) {
                if (rulePattern.isSwappingNT()) {
                    numberTranslationsInvert++;
                }
                else {
                    numberTranslationsMonotone++;
                }
            }
            numberTranslations++;
        }
        return res;
    }

    public List<GeneralPairWritable3> filter(RuleWritable source,
            ArrayWritable listTargetAndProb) {
        if (!conf.getBoolean("filter", true)) {
            List<GeneralPairWritable3> res = new ArrayList<>();
            for (int i = 0; i < listTargetAndProb.get().length; i++) {
                GeneralPairWritable3 targetAndProb =
                        (GeneralPairWritable3) listTargetAndProb.get()[i];
                res.add(new GeneralPairWritable3(new RuleWritable(source,
                        targetAndProb.getFirst()), targetAndProb.getSecond()));
            }
            return res;
        }
        if (!provenanceUnion) {
            return filter(source, listTargetAndProb, "");
        }
        List<GeneralPairWritable3> res =
                filter(source, listTargetAndProb, "");
        Set<RuleWritable> ruleSet = new HashSet<>();
        for (GeneralPairWritable3 mainRuleAndFeatures: res) {
            ruleSet.add(mainRuleAndFeatures.getFirst());
        }
        // TODO use getStrings function elsewhere
        String[] provenances = conf.getStrings("provenance");
        for (String provenance: provenances) {
            List<GeneralPairWritable3> resProvenance =
                    filter(source, listTargetAndProb, provenance);
            for (GeneralPairWritable3 ruleAndFeatures: resProvenance) {
                if (!ruleSet.contains(ruleAndFeatures.getFirst())) {
                    res.add(ruleAndFeatures);
                    ruleSet.add(ruleAndFeatures.getFirst());
                }
                // TODO else add a check that the features are the same
            }
        }
        return res;
    }
}
