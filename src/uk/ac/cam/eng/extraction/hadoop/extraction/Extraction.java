/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author jmp84 This class runs the extraction and the MapReduce features. It
 *         uses the JobControl api for workflow.
 */
public class Extraction extends Configured implements Tool {

    public int run(String[] args) throws IOException {
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
        JobControl jobControl = new JobControl("Extraction");
        ControlledJob extractorJob =
                new ControlledJob(ExtractorJob.getJob(conf), null);
        jobControl.addJob(extractorJob);
        jobControl.run();
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
