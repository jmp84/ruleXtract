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
 * @author jmp84 MapReduce job to compute average source unaligned and
 */
public class UnalignedWordJob implements MapReduceFeature {

    private final static String name = "unaligned_words";

    public int getNumberOfFeatures(Configuration conf) {
        // 2 features: average number of unaligned source words, average number
        // of unaligned target words
        return 2;
    }

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, name);
        job.setJarByClass(UnalignedWordJob.class);
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(RuleInfoWritable.class);
        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(MapWritable.class);
        // identity Mapper
        job.setMapperClass(Mapper.class);
        job.setReducerClass(UnalignedWordReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, conf.get("work_dir") + "/rules");
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir") + "/"
                + name));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Reducer to compute the unaligned word feature. For a given key (the
     * rule), loops over all values (the metadata) and compute the average
     * number of unaligned source words and target words
     */
    private static class UnalignedWordReducer extends
            Reducer<RuleWritable, RuleInfoWritable, RuleWritable, MapWritable> {

        /**
         * Starting index for this mapreduce feature. This is given by a config
         * and set in the setup method.
         */
        private static int featureStartIndex;

        // static writables to avoid memory consumption
        private static MapWritable features = new MapWritable();
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
            featureStartIndex = conf.getInt(name, 0);
        }

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(RuleWritable key,
                Iterable<RuleInfoWritable> values, Context context)
                throws IOException, InterruptedException {
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
}
