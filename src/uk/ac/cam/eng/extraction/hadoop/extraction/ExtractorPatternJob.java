
package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3PatternArrayWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritablePattern;

public class ExtractorPatternJob extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        // load the property config file
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
        conf.set("mapreduce.tasktracker.map.tasks.maximum", "6");
        Job job = Job.getInstance(new Cluster(conf), conf);
        job.setJarByClass(ExtractorPatternJob.class);
        job.setJobName("Rule Extraction");
        // needs to specify the map output key (respectively value) class
        // because
        // it is different than the final output key (respectively value) class
        // may not be needed for key
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(PairWritablePattern.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(PairWritable3PatternArrayWritable.class);
        job.setMapperClass(ExtractorMapperPattern.class);
        job.setReducerClass(ExtractorReducerPattern.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, conf.get("inputPaths"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));
        FileOutputFormat.setCompressOutput(job, true);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage args: configFile");
        }
        int res = ToolRunner.run(new ExtractorPatternJob(), args);
        System.exit(res);
    }
}
