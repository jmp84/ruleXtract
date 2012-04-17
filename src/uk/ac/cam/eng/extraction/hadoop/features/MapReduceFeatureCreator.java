/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jmp84
 */
public class MapReduceFeatureCreator {

    private static Map<String, MapReduceFeature> features = null;

    private void initFeatures() {
        features = new HashMap<>();
        features.put("source2target_probability",
                new Source2TargetProbabilityJob());
        features.put("target2source_probability",
                new Target2SourceProbabilityJob());
        features.put("source2target_lexical_probability",
                new Source2TargetLexicalProbabilityJob());
        features.put("target2source_lexical_probability",
                new Target2SourceLexicalProbabilityJob());
        features.put("source2target_pattern_probability",
                new Source2TargetPatternProbabilityJob());
        features.put("source2target_pattern_probability",
                new Source2TargetPatternProbabilityJob());
        features.put("unaligned_words", new UnalignedWordJob());
        features.put("binary_provenance", new BinaryProvenanceJob());
    }

    public MapReduceFeature getFeatureJob(String featureName) {
        if (features == null) {
            initFeatures();
        }
        if (features.containsKey(featureName)) {
            return features.get(featureName);
        }
        System.err.println("ERROR: unknown mapreduce feature: " + featureName);
        System.exit(1);
        // never reached
        return null;
    }
}
