/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84
 */
public class Target2SourceLexicalProbabilityJob implements MapReduceFeature {

    private final static String name = "target2source_lexical_probability";

    public int getNumberOfFeatures(Configuration conf) {
        return 1;
    }

    public Job getJob(Configuration conf) throws IOException {
        // add some memory to load the lex models
        // mapreduce.reduce.java.opts=-Xmx4000m
        // use a copy of the conf just in case this conf is reused elsewhere
        Configuration newconf = new Configuration(conf);
        // this is 1.0.* syntax
        // in the future it will be mapreduce.reduce.java.opts
        newconf.set("mapred.reduce.child.java.opts", "-Xmx20000m");
        Job job = new Job(newconf, name);
        // we limit the number of reducers because loading the lex models
        // takes a bit of memory
        job.setNumReduceTasks(3);
        job.setJarByClass(Target2SourceLexicalProbabilityJob.class);
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(RuleInfoWritable.class);
        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(MapWritable.class);
        // identity mapper
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Target2SourceLexicalProbabilityReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, conf.get("work_dir") + "/rules");
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir") + "/"
                + name));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Reducer
     */
    private static class Target2SourceLexicalProbabilityReducer extends
            Reducer<RuleWritable, RuleInfoWritable, RuleWritable, MapWritable> {

        /**
         * Starting index for this mapreduce feature. This is given by a config
         * and set in the setup method.
         */
        private static int featureStartIndex;

        private Target2SourceLexicalProbability2 lexModel;

        /*
         * (non-Javadoc)
         * @see
         * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
         * .Reducer.Context)
         */
        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            featureStartIndex = conf.getInt(name, 0);
            String modelFile = conf.get("target2source_lexical_model");
            lexModel = new Target2SourceLexicalProbability2(modelFile);
        }

        /*
         * (non-Javadoc)
         * @see
         * org.apache.hadoop.mapreduce.Reducer#run(org.apache.hadoop.mapreduce
         * .Reducer .Context)
         */
        @Override
        public void run(Context context) throws IOException,
                InterruptedException {
            setup(context);
            // List<RuleWritable> reducerRules = new ArrayList<>();
            // Configuration conf = context.getConfiguration();
            MapWritable features = new MapWritable();
            IntWritable featureIndex = new IntWritable(featureStartIndex);
            DoubleWritable featureValue = new DoubleWritable();
            while (context.nextKey()) {
                // reducerRules.add(WritableUtils.clone(context.getCurrentKey(),
                // conf));
                RuleWritable rule = context.getCurrentKey();
                double lexProb = lexModel.value(rule);
                featureValue.set(lexProb);
                features.put(featureIndex, featureValue);
                context.write(rule, features);
            }
            // String modelFile = conf.get("target2source_lexical_model");
            // Target2SourceLexicalProbability lexModel =
            // new Target2SourceLexicalProbability(modelFile, reducerRules);
            // MapWritable features = new MapWritable();
            // IntWritable featureIndex = new IntWritable(featureStartIndex);
            // DoubleWritable featureValue = new DoubleWritable();
            // for (RuleWritable rule: reducerRules) {
            // double lexProb = lexModel.value(rule);
            // featureValue.set(lexProb);
            // features.put(featureIndex, featureValue);
            // context.write(rule, features);
            // }
            cleanup(context);
        }
    }
}
