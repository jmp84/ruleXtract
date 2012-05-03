/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 MapReduce job to compute source-to-target probability
 */
public class Target2SourceProbabilityJob implements MapReduceFeature {

    private final static String name = "target2source_probability";

    public int getNumberOfFeatures(Configuration conf) {
        // 2 features: probability and count
        return 2;
    }

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, name);
        job.setJarByClass(Target2SourceProbabilityJob.class);
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(PairWritable.class);
        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(MapWritable.class);
        job.setMapperClass(Target2SourceProbabilityMapper.class);
        job.setReducerClass(Target2SourceProbabilityReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, conf.get("work_dir") + "/rules");
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir") + "/"
                + name));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Mapper to compute source-to-target probability. Uses method 3 descried in
     * "Fast, easy, cheap, etc." by Chris Dyer et al.
     */
    private static class Target2SourceProbabilityMapper extends
            Mapper<RuleWritable, RuleInfoWritable, RuleWritable, PairWritable> {

        // static writables to avoid memory consumption
        private final static IntWritable one = new IntWritable(1);
        private static RuleWritable sourceMarginal = new RuleWritable();
        private static RuleWritable targetMarginal = new RuleWritable();
        private static PairWritable sourceAndCount = new PairWritable();

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(RuleWritable key, RuleInfoWritable value,
                Context context) throws IOException, InterruptedException {
            if (value.hasProvenance("main")) {
                sourceMarginal.makeSourceMarginal(key, false);
                targetMarginal.makeTargetMarginal(key, false);
                sourceAndCount.set(sourceMarginal, one);
                context.write(targetMarginal, sourceAndCount);
            }
        }
    }

    /**
     * @author jmp84 Reducer to compute target-to-source probability
     */
    private static class Target2SourceProbabilityReducer extends
            Reducer<RuleWritable, PairWritable, RuleWritable, MapWritable> {

        /**
         * Starting index for this mapreduce feature. This is given by a config
         * and set in the setup method.
         */
        private static int featureStartIndex;

        // static writables to avoid memory consumption
        private static MapWritable features = new MapWritable();
        private static DoubleWritable probability = new DoubleWritable();
        private static IntWritable count = new IntWritable();

        /*
         * (non-Javadoc)
         * @see
         * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
         * .Reducer.Context)
         */
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            featureStartIndex = conf.getInt(name, 0);
        }

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(RuleWritable key, Iterable<PairWritable> values,
                Context context) throws IOException, InterruptedException {
            // first loop through the sources and gather counts
            double marginalCount = 0;
            // use HashMap because we don't need to have the rules sorted
            Map<RuleWritable, Integer> ruleCounts =
                    new HashMap<RuleWritable, Integer>();
            for (PairWritable sourceAndCount: values) {
                marginalCount += sourceAndCount.second.get();
                RuleWritable rw = new RuleWritable(sourceAndCount.first, key);
                if (!ruleCounts.containsKey(rw)) {
                    ruleCounts.put(rw, sourceAndCount.second.get());
                }
                else {
                    ruleCounts.put(rw, ruleCounts.get(rw)
                            + sourceAndCount.second.get());
                }
            }
            // do a second pass for normalization
            for (RuleWritable rw: ruleCounts.keySet()) {
                count.set(ruleCounts.get(rw));
                probability.set(count.get() / marginalCount);
                IntWritable featureIndex = new IntWritable(featureStartIndex);
                features.put(featureIndex, probability);
                featureIndex = new IntWritable(featureStartIndex + 1);
                features.put(featureIndex, count);
                Rule rule = new Rule(rw);
                // write the rules in a consistent way with the rules obtained
                // from the extraction (nonterminals inverted on the target
                // side)
                if (rule.isSwapping()) {
                    RuleWritable ruleInvertOnTheTarget =
                            new RuleWritable(rule.invertNonTerminals());
                    context.write(ruleInvertOnTheTarget, features);
                }
                else {
                    context.write(rw, features);
                }
            }
        }
    }
}
