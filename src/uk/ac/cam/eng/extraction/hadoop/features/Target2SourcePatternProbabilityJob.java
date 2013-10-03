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

/**
 * @author jmp84 MapReduce job to compute source-to-target pattern probability
 */
public class Target2SourcePatternProbabilityJob implements MapReduceFeature {

    private final static String name = "target2source_pattern_probability";

    public int getNumberOfFeatures(Configuration conf) {
        return 1;
    }

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, name);
        job.setJarByClass(Target2SourcePatternProbabilityJob.class);
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(MapWritable.class);
        job.setMapperClass(Target2SourcePatternProbabilityMapper.class);
        job.setReducerClass(Target2SourcePatternProbabilityReducer.class);
        job.setPartitionerClass(TargetPatternPartitioner.class);
        job.setGroupingComparatorClass(TargetPatternGroupingComparator.class);
        job.setSortComparatorClass(TargetPatternSortComparator.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, conf.get("work_dir") + "/rules");
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir") + "/"
                + name));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Mapper to compute the target-to-source pattern probability. Emits the
     * rule with a count of one, the pattern with a count of one and the target
     * pattern with a count of one. The partitioner, the sorting comparator and
     * the grouping comparator are modified to have all rules with the same
     * target pattern processed by the same reducer with the target pattern
     * being the smallest element followed by pattern, followed by the rules.
     */
    private static class Target2SourcePatternProbabilityMapper extends
            Mapper<RuleWritable, RuleInfoWritable, RuleWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(RuleWritable key, RuleInfoWritable value,
                Context context) throws IOException, InterruptedException {
            context.write(key, one);
            RuleWritable pattern = key.getPattern();
            context.write(pattern, one);
            RuleWritable targetPattern = key.getTargetPattern();
            context.write(targetPattern, one);
        }
    }

    /**
     * @author jmp84
     */
    private static class Target2SourcePatternProbabilityReducer extends
            Reducer<RuleWritable, IntWritable, RuleWritable, MapWritable> {

        /**
         * Starting index for this mapreduce feature. This is given by a config
         * and set in the setup method.
         */
        private static int featureStartIndex;

        // static writables to avoid memory consumption
        private static MapWritable features = new MapWritable();
        private static DoubleWritable probability = new DoubleWritable();
        private static IntWritable featureIndex = new IntWritable();

        private int targetPatternCount;
        private int patternCount;
        private Map<RuleWritable, Integer> patternsCount = new HashMap<>();

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
            featureIndex.set(featureStartIndex);
        }

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(RuleWritable key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            if (key.isPattern() && key.isTargetEmpty()) {
                targetPatternCount = 0;
                for (IntWritable value: values) {
                    targetPatternCount += value.get();
                }
            }
            else if (key.isPattern()) {
                patternCount = 0;
                for (IntWritable value: values) {
                    patternCount++;
                }
                patternsCount.put(
                        WritableUtils.clone(key, context.getConfiguration()),
                        patternCount);
            }
            else {
                RuleWritable pattern = key.getPattern();
                probability.set((double) patternsCount.get(pattern)
                        / targetPatternCount);
                features.put(featureIndex, features);
                context.write(key, features);
            }
        }
    }
}
