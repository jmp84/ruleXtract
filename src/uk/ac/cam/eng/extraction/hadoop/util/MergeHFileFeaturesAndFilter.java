/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.extraction.Extraction;

/**
 * @author jmp84 This class merges the features in an hfile by doing the dot
 *         product between the features and a feature vector and prefilters the
 *         rules according to a config
 */
public class MergeHFileFeaturesAndFilter extends Configured implements Tool {

    /**
     * @param args
     */
    public int run(String[] args) throws IOException {
        String configFile = args[0];
        // TODO make a function out of this property loading
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
        String[] stringWeights = conf.getStrings("weights");
        if (stringWeights == null) {
            System.err.println("ERROR: missing 'weights' property");
            System.exit(1);
        }
        double[] weights = new double[stringWeights.length];
        for (int i = 0; i < stringWeights.length; i++) {
            weights[i] = Double.parseDouble(stringWeights[i]);
        }
        String hfileInput = conf.get("hfile_input");
        String hfileOutput = conf.get("hfile_output");
        if (hfileInput == null || hfileOutput == null) {
            System.err.println("ERROR: missing input ('hfile_input') or " +
                    "output ('hfile_output') hfile property");
            System.exit(1);
        }
        FileSystem fs = FileSystem.get(conf);
        HFile.Reader hfileReader =
                HFile.createReader(fs, new Path(hfileInput), new CacheConfig(
                        conf));
        hfileReader.loadFileInfo();
        HFileScanner scanner = hfileReader.getScanner(false, false);
        scanner.seekTo();
        HFile.WriterFactory hfileWriterFactory = HFile.getWriterFactory(conf);
        Path path = new Path(hfileOutput);
        HFile.Writer hfileWriter =
                hfileWriterFactory
                        .createWriter(fs, path, 64 * 1024, "gz", null);
        do {

        }
        while (scanner.next());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Args: <configFile>");
            System.exit(1);
        }
        int res = ToolRunner.run(new Extraction(), args);
        System.exit(res);
    }

}
