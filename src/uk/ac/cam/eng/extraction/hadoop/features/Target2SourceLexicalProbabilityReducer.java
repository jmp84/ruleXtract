/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84
 */
public class Target2SourceLexicalProbabilityReducer extends
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
    private static String featureName = "target2source_lexical_probability";

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
     * .Reducer.Context)
     */
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        featureStartIndex = conf.getInt(featureName, 0);
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Reducer#run(org.apache.hadoop.mapreduce.Reducer
     * .Context)
     */
    @Override
    public void run(Context context)
            throws IOException, InterruptedException {
        setup(context);
        List<RuleWritable> reducerRules = new ArrayList<>();
        Configuration conf = context.getConfiguration();
        while (context.nextKey()) {
            reducerRules.add(
                    WritableUtils.clone(context.getCurrentKey(), conf));
        }
        String modelFile = conf.get("target2source_lexical_model");
        Target2SourceLexicalProbability lexModel =
                new Target2SourceLexicalProbability(modelFile, reducerRules);
        MapWritable features = new MapWritable();
        IntWritable featureIndex = new IntWritable(featureStartIndex);
        DoubleWritable featureValue = new DoubleWritable();
        for (RuleWritable rule: reducerRules) {
            double lexProb = lexModel.value(rule);
            featureValue.set(lexProb);
            features.put(featureIndex, featureValue);
            context.write(rule, features);
        }
        cleanup(context);
    }
}
