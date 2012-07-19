/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * @author jmp84
 */
public class MapReduceFeatureCreator {

    private Map<String, MapReduceFeature> features;

    public MapReduceFeatureCreator(Configuration conf) {
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
        features.put("source2target_probability_prior",
                new Source2TargetProbabilityWithPriorJob());
        features.put("target2source_probability_prior",
                new Target2SourceProbabilityWithPriorJob());
        String provenance = conf.get("provenance");
        if (provenance != null) {
            String[] provenances = provenance.split(",");
            for (String prov: provenances) {
                features.put("provenance_source2target_probability-" + prov,
                        new ProvenanceSource2TargetProbabilityJob(prov));
                features.put("provenance_target2source_probability-" + prov,
                        new ProvenanceTarget2SourceProbabilityJob(prov));
                features.put("provenance_source2target_lexical_probability-"
                        + prov,
                        new ProvenanceSource2TargetLexicalProbabilityJob(prov));
                features.put("provenance_target2source_lexical_probability-"
                        + prov,
                        new ProvenanceTarget2SourceLexicalProbabilityJob(prov));
            }
        }
    }

    public MapReduceFeature getFeatureJob(String featureName) {
        if (features.containsKey(featureName)) {
            return features.get(featureName);
        }
        System.err.println("ERROR: unknown mapreduce feature: " + featureName);
        System.exit(1);
        // never reached
        return null;
    }
}
