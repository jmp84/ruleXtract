/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.util.Pair;

/**
 * @author juan
 */
public class HFileCreatorMergeProvenance extends Configured {

    private static byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    private static ArrayWritable bytes2ArrayWritable(ByteBuffer bytes) {
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

    private static RuleWritable bytes2RuleWritable(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        RuleWritable value = new RuleWritable();
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

    private static ArrayWritable mergeFeatures(ArrayWritable features1,
            ArrayWritable features2) {
        if (features2 == null) {
            DoubleWritable[] resArray =
                    new DoubleWritable[features1.get().length + 3];
            resArray[resArray.length - 1] = new DoubleWritable(0);
            resArray[resArray.length - 2] = new DoubleWritable(0);
            resArray[resArray.length - 3] = new DoubleWritable(0);
            ArrayWritable res = new ArrayWritable(DoubleWritable.class);
            res.set(resArray);
            return res;
        }
        if (features1 == null) {
            DoubleWritable[] resArray =
                    new DoubleWritable[features1.get().length + 3];
            resArray[resArray.length - 1] = new DoubleWritable(0);
            resArray[resArray.length - 2] = new DoubleWritable(0);
            resArray[resArray.length - 3] = new DoubleWritable(0);
            ArrayWritable res = new ArrayWritable(DoubleWritable.class);
            res.set(resArray);
            return res;        	
        }
        DoubleWritable[] resArray =
                new DoubleWritable[features1.get().length
                        + features2.get().length];
        for (int i = 0; i < features1.get().length; i++) {
            resArray[i] = (DoubleWritable) features1.get()[i];
        }
        for (int i = 0; i < features2.get().length; i++) {
            resArray[i + features1.get().length] =
                    (DoubleWritable) features2.get()[i];
        }
        ArrayWritable res = new ArrayWritable(DoubleWritable.class);
        res.set(resArray);
        return res;
    }

    private static ArrayWritable merge(ArrayWritable targetsAndFeatures,
            ArrayWritable targetsAndFeatures2) {
        ArrayWritable res = new ArrayWritable(PairWritable3.class);
        Writable[] targetsAndFeaturesArray = targetsAndFeatures.get();
        Writable[] targetsAndFeatures2Array = targetsAndFeatures2.get();
        List<PairWritable3> resList = new ArrayList<>();
        int i = 0, j = 0;
        while (i < targetsAndFeaturesArray.length
                && j < targetsAndFeatures2Array.length) {
            RuleWritable target = ((PairWritable3) targetsAndFeaturesArray[i]).first;
            RuleWritable target2 = ((PairWritable3) targetsAndFeatures2Array[j]).first;
            int cmp = target.compareYield(target2);
            ArrayWritable mergedFeatures = null;
            if (cmp == 0) {
                mergedFeatures =
		    mergeFeatures(((PairWritable3) targetsAndFeaturesArray[i]).second,
				  ((PairWritable3) targetsAndFeaturesArray[j]).second);
                i++;
                j++;
            }
            else if (cmp < 0) {
                mergedFeatures =
		    mergeFeatures(((PairWritable3) targetsAndFeaturesArray[i]).second, null);
                i++;
            }
            else {
                System.err.println(
                        "WARNING: The main HFile has a missing target: "
                                + target2);
                mergedFeatures =
            		    mergeFeatures(null, ((PairWritable3) targetsAndFeaturesArray[j]).second);
                j++;
            }
            resList.add(new PairWritable3(target, mergedFeatures));
        }
        return res;
    }

    private static void mergeHFiles(List<String> inputHFiles,
            String outputHFile, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputHFile);
        if (fs.exists(path)) {
            System.out.println("Error: " + outputHFile + " already exists");
            System.exit(1);
        }
        HFile.Writer writer = new HFile.Writer(fs, path);
        List<HFileScanner> scanners = new ArrayList<>();
        for (String inputHFile: inputHFiles) {
            HFile.Reader hfileReader =
                    new HFile.Reader(fs, new Path(inputHFile), null, false);
            hfileReader.loadFileInfo();
            HFileScanner hfileScanner = hfileReader.getScanner();
            scanners.add(hfileScanner);
        }
        List<Pair<ByteBuffer, ArrayWritable>> toBeProcessed =
                new ArrayList<>();
        // initialized toBeProcessed with the first element of each HFile
        for (int i = 0; i < scanners.size(); i++) {
            HFileScanner scanner = scanners.get(i);
            // go to the beginning of the file
            scanner.seekTo();
            // we assume here that the hfile is not empty
            // TODO find a way to check that
            ByteBuffer key = scanner.getKey();
            ArrayWritable value = bytes2ArrayWritable(scanner.getValue());
            toBeProcessed.add(new Pair<ByteBuffer, ArrayWritable>(key,
                    value));
        }
        boolean allHFilesEmpty = false;
        boolean isnonext;
        // determine if we should scan the next element. initialize to false.
        // the first element will never be true
        boolean[] nonext = new boolean[scanners.size()];
        while (!allHFilesEmpty) {
            // process toBeProcessed first
            // assume the first element corresponds to the main HFile
            Pair<ByteBuffer, ArrayWritable> mainHFileElement =
                    toBeProcessed.get(0);
            ArrayWritable merged = mainHFileElement.getSecond();
            for (int i = 1; i < toBeProcessed.size(); i++) {
                int cmp =
                        mainHFileElement.getFirst().compareTo(
                                toBeProcessed.get(i).getFirst());
                if (cmp == 0) {
                    merged = merge(merged, toBeProcessed.get(i).getSecond());
                }
                else if (cmp < 0) {
                    nonext[i] = true;
                }
                else {
                    System.err
                            .println("ERROR: The main HFile has a missing element: "
                                    + toBeProcessed.get(i).getFirst()
                                    + " from HFile " + inputHFiles.get(i));
                    System.exit(1);
                }
            }
            byte[] valueBytes = object2ByteArray(merged);
            writer.append(mainHFileElement.getFirst().array(), valueBytes);
            allHFilesEmpty = true;
            // checks whether at least one element in nonext is true, meaning
            // we need at least one more processing
            isnonext = false;
            // refill toBeProcessed as needed
            for (int i = 0; i < scanners.size(); i++) {
                HFileScanner scanner = scanners.get(i);
                // get the next element
                if (!nonext[i]) {
                    if (scanner.next()) {
                        allHFilesEmpty = false;
                        ByteBuffer key = scanner.getKey();
                        ArrayWritable value =
                                bytes2ArrayWritable(scanner.getValue());
                        toBeProcessed
                                .set(i, new Pair<ByteBuffer, ArrayWritable>(
                                        key, value));
                    }
                }
                else {
                    // at least one element remains to be processed
                    isnonext = true;
                }
            }
            // should never happen: all scanners have reached the end and there
            // are still elements to be processed
            if (allHFilesEmpty && isnonext) {
                System.err.println("ERROR: all scanners are at the end and " +
                        "there are still items to be processed");
                System.exit(1);
            }
        }
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out
                    .println("Usage: HFileCreatorMergeProvenance <config file>");
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
        String inputHFiles = p.getProperty("input_hfiles");
        if (inputHFiles == null) {
            System.err.println("ERROR: missing property input_hfiles");
            System.exit(1);
        }
        List<String> inputHFilesList = Arrays.asList(inputHFiles.split(","));
        String outputHFile = p.getProperty("output_hfile");
        if (outputHFile == null) {
            System.err.println("ERROR: missing property output_hfile");
            System.exit(1);
        }
        mergeHFiles(inputHFilesList, outputHFile, conf);
    }
}
