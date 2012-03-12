/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Reducer for the MapReduce feature merging job. Groups together
 *         the targets and merge the features.
 */
public class MapReduceFeatureMergeReducer
        extends
        Reducer<BytesWritable, GeneralPairWritable, BytesWritable, GeneralPairWritable> {

    // static writables to avoid memory consumption
    private static GeneralPairWritable targetAndFeatures =
            new GeneralPairWritable();

    /**
     * Adds key/value pairs from the second map to the first
     * 
     * @param features1
     *            The first map of features to merge
     * @param features2
     *            The second map of features to merge
     * @return features1 after adding key/value pairs from features2
     */
    private static MapWritable mergeFeatures(MapWritable features1,
            MapWritable features2) {
        for (Writable key2: features2.keySet()) {
            if (features1.containsKey(key2)) {
                System.err.println(
                        "WARNING: feature already present " + key2.toString());
                if (!features1.get(key2).equals(features2.get(key2))) {
                    System.err.println("ERROR: feature already present with " +
                            "a different value: index " + key2.toString() +
                            " value1: " + features1.get(key2).toString() +
                            " value2: " + features2.get(key2).toString());
                    System.exit(1);
                }
            }
            features1.put(key2, features2.get(key2));
        }
        return features1;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(BytesWritable key,
            Iterable<GeneralPairWritable> values, Context context)
            throws IOException, InterruptedException {
        // not necessary, but nicer to have the targets in sorted order
        Map<RuleWritable, MapWritable> targetsAndFeatures = new TreeMap<>();
        // first pass to put together the identical targets and merge their
        // features
        for (GeneralPairWritable value: values) {
            // create new object, otherwise gets overwritten
            RuleWritable target = new RuleWritable();
            target.setTarget(((RuleWritable) value.getFirst()).getTarget());
            MapWritable features = (MapWritable) value.getSecond();
            if (targetsAndFeatures.containsKey(target)) {
                features =
                        mergeFeatures(features, targetsAndFeatures.get(target));
            }
            else {
                targetsAndFeatures.put(target, features);
            }
        }
        // second pass to write to the output
        for (RuleWritable target: targetsAndFeatures.keySet()) {
            targetAndFeatures.setFirst(target);
            targetAndFeatures.setSecond(targetsAndFeatures.get(target));
            context.write(key, targetAndFeatures);
        }
    }
}
