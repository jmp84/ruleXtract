/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Reducer to compute the unaligned word feature. For a given key
 *         (the rule), loops over all values (the metadata) and compute the
 *         average number of unaligned source words and target words
 */
public class UnalignedWordReducer extends
        Reducer<RuleWritable, RuleInfoWritable, RuleWritable, MapWritable> {

    /**
     * Starting index for this mapreduce feature. This is given by a config and
     * set in the setup method.
     */
    private static int featureStartIndex;
    /**
     * Name of the feature class. This is hard coded and used to retrieve
     * featureStartIndex from a config.
     */
    private static String featureName = "unalignedWords";

    // static writables to avoid memory consumption
    private static MapWritable features = new MapWritable();
    private static IntWritable featureIndex = new IntWritable();
    private static DoubleWritable averageUnalignedSourceWords =
            new DoubleWritable();
    private static DoubleWritable averageUnalignedTargetWords =
            new DoubleWritable();

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
     * .Reducer.Context)
     */
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        // TODO add a check here
        featureStartIndex = conf.getInt(featureName, 0);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(RuleWritable key, Iterable<RuleInfoWritable> values,
            Context context) throws IOException, InterruptedException {
        int numberUnalignedSourceWords = 0, numberUnalignedTargetWords = 0;
        int numberOccurrences = 0;
        for (RuleInfoWritable ruleInfoWritable: values) {
            numberUnalignedSourceWords +=
                    ruleInfoWritable.getNumberUnalignedSourceWords();
            numberUnalignedTargetWords +=
                    ruleInfoWritable.getNumberUnalignedTargetWords();
            numberOccurrences++;
        }
        averageUnalignedSourceWords.set((double) numberUnalignedSourceWords
                / numberOccurrences);
        averageUnalignedTargetWords.set((double) numberUnalignedTargetWords
                / numberOccurrences);
        IntWritable featureIndex = new IntWritable(featureStartIndex);
        features.put(featureIndex, averageUnalignedSourceWords);
        featureIndex = new IntWritable(featureStartIndex + 1);
        features.put(featureIndex, averageUnalignedTargetWords);
        context.write(key, features);
    }
}
