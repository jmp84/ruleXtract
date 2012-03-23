/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob;

/**
 * @author jmp84
 */
public class Source2TargetLexicalProbabilityJob implements HadoopJob {

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "source2target_lexical_probability");
        job.setJarByClass(Source2TargetLexicalProbabilityJob.class);
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(RuleInfoWritable.class);
        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(MapWritable.class);
        // identity mapper
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Source2TargetLexicalProbabilityReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, conf.get("rules"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("s2t_lex")));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Reducer
     */
    private static class Source2TargetLexicalProbabilityReducer extends
            Reducer<RuleWritable, RuleInfoWritable, RuleWritable, MapWritable> {

        /**
         * Starting index for this mapreduce feature. This is given by a config
         * and set in the setup method.
         */
        private static int featureStartIndex;

        /**
         * Name of the feature class. This is hard coded and used to retrieve
         * featureStartIndex from a config.
         */
        private static String featureName = "source2target_lexical_probability";

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
         * org.apache.hadoop.mapreduce.Reducer#run(org.apache.hadoop.mapreduce
         * .Reducer .Context)
         */
        @Override
        public void run(Context context)
                throws IOException, InterruptedException {
            setup(context);
            List<RuleWritable> reducerRules = new ArrayList<>();
            Configuration conf = context.getConfiguration();
            while (context.nextKey()) {
                // reduce(context.getCurrentKey(), context.getValues(),
                // context);
                reducerRules.add(
                        WritableUtils.clone(context.getCurrentKey(), conf));
            }
            String modelFile = conf.get("source2target_lexical_model");
            Source2TargetLexicalProbability lexModel =
                    new Source2TargetLexicalProbability(modelFile, reducerRules);
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
}
