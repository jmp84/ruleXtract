/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.util.HashMap;
import java.util.Map;

import uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob;

/**
 * @author jmp84
 */
public class MapReduceFeatureCreator {

    private Map<String, HadoopJob> features;

    public MapReduceFeatureCreator() {
        features = new HashMap<>();
        features.put("source2target_probability",
                new Source2TargetProbabilityJob());
        features.put("target2source_probability",
                new Target2SourceProbabilityJob());
        features.put("binary_provenance",
                new BinaryProvenanceJob());
        features.put("source2target_lexical_probability",
                new Source2TargetLexicalProbabilityJob());
        features.put("target2source_lexical_probability",
                new Target2SourcePatternProbabilityJob());
        features.put("source2target_pattern_probability",
                new Source2TargetPatternProbabilityJob());
        features.put("source2target_pattern_probability",
                new Source2TargetPatternProbabilityJob());
        features.put("unaligned_word", new UnalignedWordJob());
    }
}
