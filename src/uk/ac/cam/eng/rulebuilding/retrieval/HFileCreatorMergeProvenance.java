/**
 * 
 */
package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author juan
 *
 */
public class HFileCreatorMergeProvenance extends Configured {

    private static ArrayWritable convertValueBytes(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        ArrayWritable value = new ArrayWritable(PairWritable3.class);
        try {
            value.readFields(in);
        }
        catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }
	
	private static void mergeHFiles(List<String> inputHFiles, String outputHFile) throws IOException {
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);        
        Path path = new Path(outputHFile);
        if (fs.exists(path)) {
            System.out.println("Error: " + outputHFile + " already exists");
            System.exit(1);
        }
        HFile.Writer writer = new HFile.Writer(fs, path);
        List<HFileScanner> scanners = new ArrayList<>();
        for (String inputHFile: inputHFiles) {
        	HFile.Reader hfileReader = new HFile.Reader(fs, new Path(inputHFile), null, false);
        	hfileReader.loadFileInfo();
        	HFileScanner hfileScanner = hfileReader.getScanner();
        	scanners.add(hfileScanner);
        }
        Stack toBeProcessed = new Stack();
        while (true) {
        	for (HFileScanner scanner: scanners) {
        		if (scanner.next()) {
        			scanner.getKey();
        			convertValueBytes(hfileScanner.getValue());
        		}
        	}
        }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: HFileCreatorMergeProvenance <config file>");
            System.exit(1);
        }
        String configFile = args[0];
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(configFile));
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = new Configuration();
        for (String prop: p.stringPropertyNames()) {
            conf.set(prop, p.getProperty(prop));
        }
        
	}
}
