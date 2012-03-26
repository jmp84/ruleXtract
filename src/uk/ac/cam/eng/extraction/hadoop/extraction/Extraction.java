/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.features.MapReduceFeatureCreator;
import uk.ac.cam.eng.extraction.hadoop.features.MapReduceFeatureMergeJob;
import uk.ac.cam.eng.extraction.hadoop.util.ExtractorDataLoader;
import uk.ac.cam.eng.extraction.hadoop.util.Util;

/**
 * @author jmp84 This class runs the extraction and the MapReduce features. It
 *         uses the JobControl api for workflow.
 */
public class Extraction extends Configured implements Tool {

    /**
     * Utility to convert the output SequenceFile of the MapReduce feature merge
     * job into an HFile. Note that there is only one input SequenceFile because
     * there is only one reducer of the merge job so that we are sure that the
     * SequenceFile output is sorted.
     */
    private void sequenceFile2HFile(Configuration conf) throws IOException {
        String input = conf.get("mapreduce_features_merge");
        String output = conf.get("hfile");
        System.out.println("Reading " + input + " and writing hfile to "
                + output);
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader sequenceReader =
                new SequenceFile.Reader(fs, new Path(input), conf);
        Path path = new Path(output);
        if (fs.exists(path)) {
            System.out.println("ERROR: " + output + " already exists");
            System.exit(1);
        }
        HFile.WriterFactory hfileWriterFactory = HFile.getWriterFactory(conf);
        HFile.Writer hfileWriter =
                hfileWriterFactory
                        .createWriter(fs, path, 64 * 1024, "gz", null);
        BytesWritable key = new BytesWritable();
        ArrayWritable value = new ArrayWritable(GeneralPairWritable3.class);
        while (sequenceReader.next(key, value)) {
            byte[] valueBytes = Util.object2ByteArray(value);
            hfileWriter.append(key.getBytes(), valueBytes);
        }
        hfileWriter.close();
    }

    public int run(String[] args) throws IOException {
        // read and parse the config
        String configFile = args[0];
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(configFile));
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = getConf();
        for (String prop: p.stringPropertyNames()) {
            conf.set(prop, p.getProperty(prop));
        }
        // working hdfs directory that will contain the rules and the mapreduce
        // features
        String workDir = conf.get("work_dir");
        if (workDir == null) {
            System.err.println("ERROR: missing working directory (work_dir)");
            System.exit(1);
        }
        // create input data
        ExtractorDataLoader extractorDataLoader = new ExtractorDataLoader();
        String wordAlignmentFile = conf.get("acn");
        String sentenceAlignmentFile = conf.get("snt");
        String hdfsName = workDir + "/training_data";
        if (wordAlignmentFile == null || sentenceAlignmentFile == null
                || hdfsName == null) {
            System.err.println("ERROR: missing property for creating input " +
                    "training data (acn, snt or training_data)");
            System.exit(1);
        }
        extractorDataLoader.acn2hadoop(
                sentenceAlignmentFile, wordAlignmentFile, hdfsName);
        // set up the extraction job
        JobControl jobControl = new JobControl("Extraction");
        HadoopJob extractorJob = new ExtractorJob();
        ControlledJob controlledExtractorJob =
                new ControlledJob(extractorJob.getJob(conf), null);
        jobControl.addJob(controlledExtractorJob);
        List<ControlledJob> extractionHold = new ArrayList<>();
        extractionHold.add(controlledExtractorJob);
        // set up the mapreduce feature jobs
        List<ControlledJob> mapreduceFeaturesHold = new ArrayList<>();
        String mapreduceFeatures = conf.get("mapreduce_features");
        if (mapreduceFeatures == null) {
            System.err.println("ERROR: no mapreduce feature set");
            System.exit(1);
        }
        String[] mapreduceFeaturesArray = mapreduceFeatures.split(",");
        MapReduceFeatureCreator featureCreator = new MapReduceFeatureCreator();
        for (String mapreduceFeature: mapreduceFeaturesArray) {
            HadoopJob featureJob =
                    featureCreator.getFeatureJob(mapreduceFeature);
            ControlledJob controlledFeatureJob =
                    new ControlledJob(featureJob.getJob(conf), extractionHold);
            jobControl.addJob(controlledFeatureJob);
            mapreduceFeaturesHold.add(controlledFeatureJob);
        }
        // set up the merge job
        HadoopJob mergeJob = new MapReduceFeatureMergeJob();
        ControlledJob controlledMergeJob =
                new ControlledJob(mergeJob.getJob(conf), mapreduceFeaturesHold);
        jobControl.addJob(controlledMergeJob);
        // kick off jobs
        jobControl.run();
        sequenceFile2HFile(conf);
        // TODO what to return ?
        return jobControl.allFinished() ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage args: configFile");
            System.exit(1);
        }
        int res = ToolRunner.run(new Extraction(), args);
        System.exit(res);
    }
}