/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;

/**
 * @author jmp84 This class creates a set of features given a list of rules
 */
public class FeatureCreator {

    // list of all features
    private Map<String, Feature> features;
    // list of selected features in order
    private String[] selectedFeatures;

    public FeatureCreator(String source2targetLexicalModel,
            String target2sourceLexicalModel, List<PairWritable3> rules,
            String[] selectedFeatures)
            throws FileNotFoundException, IOException {
        features = new HashMap<String, Feature>();
        features.put("source2target_probability",
                new Source2TargetProbability());
        features.put("target2source_probability",
                new Target2SourceProbability());
        features.put("word_insertion_penalty", new WordInsertionPenalty());
        features.put("rule_insertion_penalty", new RuleInsertionPenalty());
        features.put("glue_rule", new GlueRule());
        features.put("reorder_scale", new ReorderScale());
        features.put("insert_scale", new InsertScale());
        features.put("rule_count_1", new RuleCount1());
        features.put("rule_count_2", new RuleCount2());
        features.put("rule_count_greater_than_2", new RuleCountGreaterThan2());
        features.put("source2target_lexical_probability",
                new Source2TargetLexicalProbability(source2targetLexicalModel,
                        rules));
        features.put("target2source_lexical_probability",
                new Target2SourceLexicalProbability(target2sourceLexicalModel,
                        rules));
        this.selectedFeatures = selectedFeatures;
    }

    /*
     * public FeatureCreator(String listFeatureFile) throws
     * FileNotFoundException, IOException { featureNames = new
     * ArrayList<String>(); try (BufferedReader br = new BufferedReader(new
     * FileReader(listFeatureFile))) { String line; while ((line =
     * br.readLine()) != null) { if (line.matches("^#")) { continue; } line =
     * line.trim(); featureNames.add(line); } } }
     */

    private double createFeature(String featureName,
            PairWritable3 ruleAndMapReduceFeatures) {
        return features.get(featureName).value(
                new Rule(ruleAndMapReduceFeatures.first),
                ruleAndMapReduceFeatures.second);
    }

    private double createFeatureAsciiOovDeletion(String featureName,
            PairWritable3 asciiOovDeletionRule) {
        return features.get(featureName).valueAsciiOovDeletion(
                new Rule(asciiOovDeletionRule.first),
                asciiOovDeletionRule.second);
    }

    private double createFeatureGlueRule(String featureName,
            PairWritable3 glueRule) {
        return features.get(featureName).valueGlue(new Rule(glueRule.first),
                glueRule.second);
    }

    private PairWritable3
            createFeatures(PairWritable3 ruleAndMapReduceFeatures) {
        PairWritable3 res = new PairWritable3();
        res.first = ruleAndMapReduceFeatures.first;
        DoubleWritable[] featureValues = new DoubleWritable[features.size()];
        int i = 0;
        for (String featureName: selectedFeatures) {
            double featureValue =
                    createFeature(featureName, ruleAndMapReduceFeatures);
            featureValues[i] = new DoubleWritable(featureValue);
            i++;
        }
        res.second = new ArrayWritable(DoubleWritable.class, featureValues);
        return res;
    }

    private PairWritable3 createFeaturesAsciiOovDeletion(
            PairWritable3 asciiOovDeletionRule) {
        PairWritable3 res = new PairWritable3();
        res.first = asciiOovDeletionRule.first;
        DoubleWritable[] featureValues = new DoubleWritable[features.size()];
        int i = 0;
        for (String featureName: selectedFeatures) {
            double featureValue =
                    createFeatureAsciiOovDeletion(featureName,
                            asciiOovDeletionRule);
            featureValues[i] = new DoubleWritable(featureValue);
            i++;
        }
        res.second = new ArrayWritable(DoubleWritable.class, featureValues);
        return res;
    }

    private PairWritable3 createFeaturesGlueRule(PairWritable3 glueRule) {
        PairWritable3 res = new PairWritable3();
        res.first = glueRule.first;
        DoubleWritable[] featureValues = new DoubleWritable[features.size()];
        int i = 0;
        for (String featureName: selectedFeatures) {
            double featureValue =
                    createFeatureGlueRule(featureName, glueRule);
            featureValues[i] = new DoubleWritable(featureValue);
            i++;
        }
        res.second = new ArrayWritable(DoubleWritable.class, featureValues);
        return res;
    }

    public List<PairWritable3> createFeatures(
            List<PairWritable3> rulesAndMapReduceFeatures) {
        List<PairWritable3> res = new ArrayList<PairWritable3>();
        for (PairWritable3 ruleAndMapReduceFeatures: rulesAndMapReduceFeatures) {
            PairWritable3 ruleAndFeatures =
                    createFeatures(ruleAndMapReduceFeatures);
            res.add(ruleAndFeatures);
        }
        return res;
    }

    public List<PairWritable3> createFeaturesAsciiOovDeletion(
            List<PairWritable3> asciiOovDeletionRules) {
        List<PairWritable3> res = new ArrayList<PairWritable3>();
        for (PairWritable3 asciiOovDeletionRule: asciiOovDeletionRules) {
            PairWritable3 asciiOovDeletionRuleAndFeatures =
                    createFeaturesAsciiOovDeletion(asciiOovDeletionRule);
            res.add(asciiOovDeletionRuleAndFeatures);
        }
        return res;
    }

    public List<PairWritable3> createFeaturesGlueRules(
            List<PairWritable3> glueRules) {
        List<PairWritable3> res = new ArrayList<PairWritable3>();
        for (PairWritable3 glueRule: glueRules) {
            PairWritable3 glueRuleAndFeatures =
                    createFeaturesGlueRule(glueRule);
            res.add(glueRuleAndFeatures);
        }
        return res;
    }
}
