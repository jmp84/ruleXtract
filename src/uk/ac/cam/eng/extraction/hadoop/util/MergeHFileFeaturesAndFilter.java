/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.rulebuilding.features.FeatureCreator;
import uk.ac.cam.eng.rulebuilding.retrieval.RuleFilter;

/**
 * @author jmp84 This class merges the features in an hfile by doing the dot
 *         product between the features and a feature vector and prefilters the
 *         rules according to a config
 */
public class MergeHFileFeaturesAndFilter extends Configured implements Tool {

    private SortedMapWritable dotProduct(double[] weights,
            SortedMapWritable features) {
        SortedMapWritable res = new SortedMapWritable();
        double score = 0;
        for (int i = 0; i < weights.length; i++) {
            IntWritable key = new IntWritable(i);
            if (features.containsKey(key)) {
                score +=
                        weights[i] * ((DoubleWritable) features.get(key)).get();
            }
        }
        res.put(new IntWritable(0), new DoubleWritable(score));
        return res;
    }

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
        String filterConfig = conf.get("filter_config");
        if (filterConfig == null) {
            System.err
                    .println("Missing property 'filter_config' in the config");
            System.exit(1);
        }
        RuleFilter ruleFilter = new RuleFilter(conf);
        ruleFilter.loadConfig(filterConfig);
        FeatureCreator featureCreator = new FeatureCreator(conf);
        String hfileInput = conf.get("hfile");
        String hfileOutput = conf.get("hfile_output");
        if (hfileInput == null || hfileOutput == null) {
            System.err.println("ERROR: missing input ('hfile_input') or "
                    + "output ('hfile_output') hfile property");
            System.exit(1);
        }
        FileSystem fs = FileSystem.get(conf);
        HFile.WriterFactory hfileWriterFactory = HFile.getWriterFactory(conf);
        Path path = new Path(hfileOutput);
        HFile.Writer hfileWriter =
                hfileWriterFactory
                        .createWriter(fs, path, 64 * 1024, "gz", null);
        HFile.Reader hfileReader =
                HFile.createReader(fs, new Path(hfileInput), new CacheConfig(
                        conf));
        hfileReader.loadFileInfo();
        HFileScanner scanner = hfileReader.getScanner(false, false);
        scanner.seekTo();
        do {
            RuleWritable key = Util.bytes2RuleWritable(scanner.getKey());
            ArrayWritable value = Util.bytes2ArrayWritable(scanner.getValue());
            List<GeneralPairWritable3> filteredRules =
                    ruleFilter.filter(key, value);
            List<GeneralPairWritable3> rulesWithFeatures =
                    featureCreator.createFeatures(filteredRules);
            if (!rulesWithFeatures.isEmpty()) {
                RuleWritable source = new RuleWritable();
                source.makeSourceMarginal(rulesWithFeatures.get(0).getFirst());
                byte[] sourceByteArray = Util.object2ByteArray(source);
                ArrayWritable targetsAndFeatures =
                        new ArrayWritable(GeneralPairWritable3.class);
                Writable[] targetsAndFeaturesArray =
                        new GeneralPairWritable3[rulesWithFeatures.size()];
                for (int i = 0; i < rulesWithFeatures.size(); i++) {
                    RuleWritable target = new RuleWritable();
                    target.makeTargetMarginal(
                            rulesWithFeatures.get(i).getFirst());
                    SortedMapWritable score =
                            dotProduct(weights, rulesWithFeatures.get(i)
                                    .getSecond());
                    targetsAndFeaturesArray[i] =
                            new GeneralPairWritable3(target, score);
                }
                targetsAndFeatures.set(targetsAndFeaturesArray);
                byte[] targetsAndFeaturesBytes =
                        Util.object2ByteArray(targetsAndFeatures);
                hfileWriter.append(sourceByteArray, targetsAndFeaturesBytes);
            }
        }
        while (scanner.next());
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Args: <configFile>");
            System.exit(1);
        }
        int res = ToolRunner.run(new MergeHFileFeaturesAndFilter(), args);
        System.exit(res);
    }

}
