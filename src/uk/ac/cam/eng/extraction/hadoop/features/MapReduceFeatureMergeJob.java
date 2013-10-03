/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.InputSampler;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
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
 *         part TODO update doc
 */
public class MapReduceFeatureMergeJob implements HadoopJob {

    public Job getJob(Configuration conf) throws IOException {
        String name = "merge";
        // add some memory
        Configuration newconf = new Configuration(conf);
        newconf.set("mapred.reduce.child.java.opts", "-Xmx20000m");
        // keep sorted before dumping to hfile: either have one reduce task or
        // use a partitioner to keep output sorted
        String partitionInput = conf.get("partition");
        Job job = new Job(newconf, name);
        job.setJarByClass(MapReduceFeatureMergeJob.class);
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(GeneralPairWritable2.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ArrayWritable.class);
        job.setMapperClass(MapReduceFeatureMergeMapper.class);
        job.setReducerClass(MapReduceFeatureMergeReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        if (partitionInput != null) {
            try {
                InputSampler.Sampler<BytesWritable, NullWritable> sampler =
                        new InputSampler.RandomSampler<>(0.1, 10000, 10);
                String partitionFile = partitionInput + "_partitionned";
                job.getConfiguration().set(
                        TotalOrderPartitioner.PARTITIONER_PATH, partitionFile);
                job.setPartitionerClass(TotalOrderPartitioner.class);
                // add partitionInput as an input to the job for sampling
                // then remove it
                FileInputFormat.setInputPaths(job, new Path(partitionInput));
                InputSampler.writePartitionFile(job, sampler);
            }
            catch (ClassNotFoundException | InterruptedException e) {
                e.printStackTrace();
                System.err.println(
                        "WARNING: reverting to only one reduce task");
                job.setNumReduceTasks(1);
            }
            // remove partitionInput from FileInputFormat: create an empty
            // sequence file and set it as an input
            // TODO when upgrade to version 2.* or greater replace
            // this hack with the use of unset
            FileSystem fs = FileSystem.get(conf);
            Path emptyInput = new Path(conf.get("work_dir") + "/emptyInput");
            SequenceFile.Writer writer =
                    new SequenceFile.Writer(fs, conf, emptyInput,
                            RuleWritable.class, MapWritable.class);
            writer.close();
            FileInputFormat.setInputPaths(job, emptyInput);
        }
        else {
            job.setNumReduceTasks(1);
        }
        String mapreduceFeatures = conf.get("mapreduce_features");
        String[] mapreduceFeaturesArray = mapreduceFeatures.split(",");
        for (String mapreduceFeature: mapreduceFeaturesArray) {
            if (mapreduceFeature.equals(
                    "provenance_source2target_lexical_probability")
                    || mapreduceFeature
                            .equals("provenance_target2source_lexical_probability")
                    || mapreduceFeature
                            .equals("provenance_source2target_probability")
                    || mapreduceFeature
                            .equals("provenance_target2source_probability")) {
                for (String provenance: conf.get("provenance").split(",")) {
                    FileInputFormat.addInputPath(job,
                            new Path(conf.get("work_dir") + "/" +
                                    mapreduceFeature + "-" + provenance));
                }
            }
            else {
                FileInputFormat.addInputPath(job, new Path(conf.get("work_dir")
                        + "/" + mapreduceFeature));
            }
        }
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir") + "/"
                + name));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Mapper
     */
    private static class MapReduceFeatureMergeMapper
            extends
            Mapper<RuleWritable, MapWritable, BytesWritable, GeneralPairWritable2> {

        private RuleWritable source = new RuleWritable();
        private BytesWritable sourceBytesWritable = new BytesWritable();
        private RuleWritable target = new RuleWritable();
        private GeneralPairWritable2 targetAndFeatures =
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
            source.makeSourceMarginal(key);
            target.makeTargetMarginal(key);
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
        private ArrayWritable targetsAndFeaturesOutputValue =
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
                                        + key2.toString() + " value1: "
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
