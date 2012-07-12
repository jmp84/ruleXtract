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

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 MapReduce job to compute source-to-target probability with a
 *         prior. We use a Dirichlet prior where the hyperparameters are
 *         min(|src|, |trg|)/max(|src|, |trg|) to favor balanced translations.
 *         We do maximum a posteriori estimation, so the s2t probability is
 *         simply relative frequency with pseudo counts given by the
 *         hyperparameters
 */
public class Source2TargetProbabilityWithPriorJob implements MapReduceFeature {

    private final static String name = "source2target_probability_prior";

    public int getNumberOfFeatures(Configuration conf) {
        // 2 features: the probability and the count
        return 2;
    }

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, name);
        job.setJarByClass(Source2TargetProbabilityWithPriorJob.class);
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(PairWritable.class);
        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(MapWritable.class);
        job.setMapperClass(Source2TargetProbabilityWithPriorMapper.class);
        job.setReducerClass(Source2TargetProbabilityWithPriorReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, conf.get("work_dir") + "/rules");
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir") + "/"
                + name));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Mapper to compute source-to-target probability. Uses method 3 described
     * in "Fast, easy, cheap, etc." by Chris Dyer et al.
     */
    private static class Source2TargetProbabilityWithPriorMapper extends
            Mapper<RuleWritable, RuleInfoWritable, RuleWritable, PairWritable> {

        private final static IntWritable one = new IntWritable(1);
        private RuleWritable sourceMarginal = new RuleWritable();
        private RuleWritable targetMarginal = new RuleWritable();
        private PairWritable targetAndCount = new PairWritable();

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(RuleWritable key, RuleInfoWritable value,
                Context context) throws IOException, InterruptedException {
            if (value.hasProvenance("main")) {
                sourceMarginal.makeSourceMarginal(key);
                targetMarginal.makeTargetMarginal(key);
                targetAndCount.set(targetMarginal, one);
                context.write(sourceMarginal, targetAndCount);
            }
        }
    }

    /**
     * Reducer to compute source-to-target probability
     */
    private static class Source2TargetProbabilityWithPriorReducer extends
            Reducer<RuleWritable, PairWritable, RuleWritable, MapWritable> {

        /**
         * Starting index for this mapreduce feature. This is given by a config
         * and set in the setup method.
         */
        private static int featureStartIndex;

        private MapWritable features = new MapWritable();
        private DoubleWritable probability = new DoubleWritable();
        private DoubleWritable count = new DoubleWritable();

        /**
         * Utility method to compute a pseudo-count.
         * 
         * @param srcLength
         * @param trgLength
         * @return
         */
        private double minMaxPseudoCount(int srcLength, int trgLength) {
            return ((double) Math.min(srcLength, trgLength))
                    / ((double) Math.max(srcLength, trgLength));
        }

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
            // first loop through the targets and gather counts
            double marginalCount = 0;
            int sourceLength = key.getSourceLength();
            // use HashMap because we don't need to have the rules sorted
            Map<RuleWritable, Double> ruleCounts = new HashMap<>();
            for (PairWritable targetAndCount: values) {
                marginalCount += targetAndCount.second.get();
                RuleWritable rw = new RuleWritable(key, targetAndCount.first);
                if (!ruleCounts.containsKey(rw)) {
                    int targetLength = rw.getTargetLength();
                    double pseudoCount =
                            minMaxPseudoCount(sourceLength, targetLength);
                    // we add the pseudo count only once per rule
                    ruleCounts.put(rw, targetAndCount.second.get()
                            + pseudoCount);
                    marginalCount += pseudoCount;
                }
                else {
                    ruleCounts.put(rw, ruleCounts.get(rw)
                            + targetAndCount.second.get());
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
                context.write(rw, features);
            }
        }
    }
}
