/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable2;
import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob;
import uk.ac.cam.eng.extraction.hadoop.util.Util;

/**
 * @author jmp84 MapReduce job that takes the output of all MapReduce features
 *         and converts them to an HFile that will be processed by the retrieval
 *         part
 */
public class MapReduceFeatureMergeJob implements HadoopJob {

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "MapReduce Features Merge");
        job.setJarByClass(MapReduceFeatureMergeJob.class);
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(GeneralPairWritable2.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ArrayWritable.class);
        job.setMapperClass(MapReduceFeatureMergeMapper.class);
        job.setReducerClass(MapReduceFeatureMergeReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // TODO loop over the features
        FileInputFormat.setInputPaths(job, conf.get("inputPaths"));
        FileOutputFormat.setOutputPath(job,
                new Path(conf.get("merged_mr_features")));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Mapper
     */
    private static class MapReduceFeatureMergeMapper
            extends
            Mapper<RuleWritable, MapWritable, BytesWritable, GeneralPairWritable2> {

        private static RuleWritable source = new RuleWritable();
        private static BytesWritable sourceBytesWritable = new BytesWritable();
        private static RuleWritable target = new RuleWritable();
        private static GeneralPairWritable2 targetAndFeatures =
                new GeneralPairWritable2();

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void
                map(RuleWritable key, MapWritable value, Context context)
                        throws IOException, InterruptedException {
            source.setSource(key.getSource());
            target.setTarget(key.getTarget());
            targetAndFeatures.setFirst(target);
            targetAndFeatures.setSecond(value);
            byte[] sourceByteArray = Util.object2ByteArray(source);
            sourceBytesWritable.set(sourceByteArray, 0, sourceByteArray.length);
            context.write(sourceBytesWritable, targetAndFeatures);
        }
    }

    /**
     * Reducer for the MapReduce feature merging job. Groups together the
     * targets and merge the features.
     */
    private static class MapReduceFeatureMergeReducer
            extends
            Reducer<BytesWritable, GeneralPairWritable2, BytesWritable, ArrayWritable> {

        // static writables to avoid memory consumption
        private static ArrayWritable targetsAndFeaturesOutputValue =
                new ArrayWritable(GeneralPairWritable3.class);

        /**
         * Adds key/value pairs from the second map to the first. The first map
         * is sorted while the second map is not. This is because when we merge
         * features we want to have them in sorted order but using a
         * SortedMapWritable for the MapReduce features is too slow.
         * 
         * @param features1
         *            The first map of features to merge
         * @param features2
         *            The second map of features to merge
         * @return features1 after adding key/value pairs from features2
         */
        private static void mergeFeatures(SortedMapWritable features1,
                MapWritable features2) {
            for (Writable key2: features2.keySet()) {
                if (features1.containsKey(key2)) {
                    System.err.println("WARNING: feature already present "
                            + key2.toString());
                    if (!features1.get(key2).equals(features2.get(key2))) {
                        System.err
                                .println("ERROR: feature already present with "
                                        + "a different value: index "
                                        + key2.toString()
                                        + " value1: "
                                        + features1.get(key2).toString()
                                        + " value2: "
                                        + features2.get(key2).toString());
                        System.exit(1);
                    }
                }
                else {
                    features1.put((WritableComparable) key2,
                            features2.get(key2));
                }
            }
        }

        private static SortedMapWritable sort(MapWritable map) {
            SortedMapWritable res = new SortedMapWritable();
            for (Writable key: map.keySet()) {
                // warning due to IntWritable not implementing
                // WritableComparable<>
                // but implementing WritableComparable
                res.put((WritableComparable) key, map.get(key));
            }
            return res;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(BytesWritable key,
                Iterable<GeneralPairWritable2> values, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            // not necessary, but nicer to have the targets in sorted order
            Map<RuleWritable, SortedMapWritable> targetsAndFeatures =
                    new TreeMap<>();
            // first pass to put together the identical targets and merge their
            // features
            for (GeneralPairWritable2 value: values) {
                // clone object, otherwise gets overwritten
                RuleWritable target =
                        WritableUtils.clone(value.getFirst(), conf);
                // clone object, otherwise gets overwritten
                AbstractMapWritable features =
                        WritableUtils.clone(value.getSecond(), conf);
                if (targetsAndFeatures.containsKey(target)) {
                    // TODO check that this function modifies targetsAndFeatures
                    mergeFeatures(targetsAndFeatures.get(target),
                            (MapWritable) features);
                }
                else {
                    // sort
                    SortedMapWritable featuresSorted =
                            sort((MapWritable) features);
                    targetsAndFeatures.put(target, featuresSorted);
                }
            }
            // second pass to write to the output
            Writable[] outputValueArray =
                    new GeneralPairWritable3[targetsAndFeatures.size()];
            int i = 0;
            for (RuleWritable target: targetsAndFeatures.keySet()) {
                outputValueArray[i] =
                        new GeneralPairWritable3(target,
                                targetsAndFeatures.get(target));
                i++;
            }
            targetsAndFeaturesOutputValue.set(outputValueArray);
            context.write(key, targetsAndFeaturesOutputValue);
        }
    }
}
