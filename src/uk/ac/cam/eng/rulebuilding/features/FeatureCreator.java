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
import org.apache.hadoop.io.DoubleWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;

/**
 * @author jmp84 This class creates a set of features given a list of rules
 */
public class FeatureCreator {

    private List<String> featureNames;

    public FeatureCreator(String listFeatureFile) throws FileNotFoundException,
            IOException {
        featureNames = new ArrayList<String>();
        try (BufferedReader br =
                new BufferedReader(new FileReader(listFeatureFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.matches("^#")) {
                    continue;
                }
                line = line.trim();
                featureNames.add(line);
            }
        }
    }

    private double createFeature(String featureName,
            PairWritable3 ruleAndMapReduceFeatures) {
        Class<?> featureClass = null;
        try {
            featureClass = Class.forName(featureName);
        }
        catch (ClassNotFoundException e) {
            System.err.println("Class not found: " + featureName);
            System.exit(1);
        }
        Feature feature = null;
        try {
            feature = (Feature) featureClass.newInstance();
        }
        catch (IllegalAccessException e) {
            System.err.println("Class not accessible: " + featureName);
            System.exit(1);
        }
        catch (InstantiationException e) {
            System.err.println("Class not instantiable: " + featureName);
            System.exit(1);
        }
        return feature.value(new Rule(ruleAndMapReduceFeatures.first),
                ruleAndMapReduceFeatures.second);
    }

    private PairWritable3
            createFeatures(PairWritable3 ruleAndMapReduceFeatures) {
        PairWritable3 res = new PairWritable3();
        res.first = ruleAndMapReduceFeatures.first;
        DoubleWritable[] mapReduceFeatures =
                (DoubleWritable[]) ruleAndMapReduceFeatures.second.get();
        DoubleWritable[] features = new DoubleWritable[featureNames.size()];
        int i = 0;
        for (String featureName: featureNames) {
            double featureValue =
                    createFeature(featureName, ruleAndMapReduceFeatures);
            features[i] = new DoubleWritable(featureValue);
            i++;
        }
        res.second = new ArrayWritable(DoubleWritable.class, features);
        return res;
    }

    public List<PairWritable3> createFeatures(
            List<PairWritable3> rulesAndMapReduceFeatures) {
        List<PairWritable3> res = new ArrayList<PairWritable3>();
        for (PairWritable3 ruleAndMapReduceFeatures: rulesAndMapReduceFeatures) {
            PairWritable3 ruleAndFeatures =
                    createFeatures(ruleAndMapReduceFeatures);
        }
        return res;
    }

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        // TODO maybe have a command line argument parser. may not be necessary
        // as there is nothing complex here.
        if (args.length != 2) {
            System.err.println("Argument required: <e2f lexical model> " +
                    "<f2e lexical model>");
        }
        // list of features
        // vtouscale,utovscale,wip,pip,gluerulescale,reorderscale,insertscale,
        // ppcount1scale,ppcount2scale,ppcountgt2scale,lexutovscale,lexvtouscale
        Map<String, Feature> features = new HashMap<String, Feature>();
        features.put("source2target_probability",
                new Source2TargetProbability());
        features.put("target2source_probability",
                new Target2SourceProbability());
        features.put("word_insertion_penalty", new WordInsertionPenalty());
        features.put("rule_insertion_penalty", new RuleInsertionPenalty());
        features.put("glue_rule", new GlueRule());
        features.put("reorder_scale", new ReorderScale());
        features.put("rule_count_1", new RuleCount1());
        features.put("rule_count_2", new RuleCount2());
        features.put("rule_count_greater_than_2", new RuleCountGreaterThan2());
        features.put("source2target_lexical_probability",
                new Source2TargetLexicalProbability(args[0]));
        features.put("target2source_lexical_probability",
                new Target2SourceLexicalProbability(args[1]));
    }
}