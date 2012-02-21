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
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.RawComparator;
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
            ArrayWritable features2, int offset) {
        if (features2 == null) {
            DoubleWritable[] resArray =
                    new DoubleWritable[features1.get().length + 3];
            for (int i = 0; i < features1.get().length; i++) {
                resArray[i] = (DoubleWritable) features1.get()[i];
            }
            resArray[resArray.length - 1] = new DoubleWritable(0);
            resArray[resArray.length - 2] = new DoubleWritable(0);
            resArray[resArray.length - 3] = new DoubleWritable(0);
            ArrayWritable res = new ArrayWritable(DoubleWritable.class);
            res.set(resArray);
            return res;
        }
        if (features1 == null) {
            DoubleWritable[] resArray =
                    new DoubleWritable[offset + 3];
            for (int i = 0; i < offset; i++) {
                resArray[i] = new DoubleWritable(0);
            }
            resArray[resArray.length - 1] = (DoubleWritable) features2.get()[2];
            resArray[resArray.length - 2] = (DoubleWritable) features2.get()[1];
            resArray[resArray.length - 3] = (DoubleWritable) features2.get()[0];
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

    private static List<PairWritable3> merge(
            List<PairWritable3> targetsAndFeatures,
            ArrayWritable targetsAndFeatures2, int index) {
        List<PairWritable3> res = new ArrayList<PairWritable3>();
        Writable[] targetsAndFeatures2Array = targetsAndFeatures2.get();
        int offset = (index == 0) ? 0 : 5 + index * 3;
        if (targetsAndFeatures.isEmpty()) {
            for (int i = 0; i < targetsAndFeatures2Array.length; i++) {
                PairWritable3 targetAndFeatures2 =
                        (PairWritable3) targetsAndFeatures2Array[i];
                ArrayWritable mergedFeatures =
                        mergeFeatures(null, targetAndFeatures2.second, offset);
                res.add(new PairWritable3(
                        targetAndFeatures2.first,
                        mergedFeatures));
            }
            return res;
        }
        int i = 0, j = 0;
        while (i < targetsAndFeatures.size()
                && j < targetsAndFeatures2Array.length) {
            RuleWritable target = targetsAndFeatures.get(i).first;
            RuleWritable target2 =
                    ((PairWritable3) targetsAndFeatures2Array[j]).first;
            ArrayWritable features = targetsAndFeatures.get(i).second;
            ArrayWritable features2 =
                    ((PairWritable3) targetsAndFeatures2Array[j]).second;
            if (features.get().length < offset) {
                DoubleWritable[] featuresArray = new DoubleWritable[offset];
                for (int k = 0; k < features.get().length; k++) {
                    featuresArray[k] = (DoubleWritable) features.get()[k];
                }
                for (int k = features.get().length; k < offset; k++) {
                    featuresArray[k] = new DoubleWritable(0);
                }
                features.set(featuresArray);
            }
            else if (features.get().length > offset) {
                System.err.println(
                        "ERROR: number of features greater than offset");
                System.exit(1);
            }
            int cmp = target.compareYield(target2);
            ArrayWritable mergedFeatures = null;
            if (cmp == 0) {
                mergedFeatures = mergeFeatures(features, features2, offset);
                i++;
                j++;
                res.add(new PairWritable3(target, mergedFeatures));
            }
            else if (cmp < 0) {
                mergedFeatures = mergeFeatures(features, null, offset);
                i++;
                res.add(new PairWritable3(target, mergedFeatures));
            }
            else {
                System.err.println(
                        "WARNING: The main HFile has a missing target: "
                                + target2);
                mergedFeatures = mergeFeatures(null, features2, offset);
                j++;
                res.add(new PairWritable3(target2, mergedFeatures));
            }
        }
        while (i < targetsAndFeatures.size()) {
            RuleWritable target = targetsAndFeatures.get(i).first;
            ArrayWritable features = targetsAndFeatures.get(i).second;
            if (features.get().length < offset) {
                DoubleWritable[] featuresArray = new DoubleWritable[offset];
                for (int k = 0; k < features.get().length; k++) {
                    featuresArray[k] = (DoubleWritable) features.get()[k];
                }
                for (int k = features.get().length; k < offset; k++) {
                    featuresArray[k] = new DoubleWritable(0);
                }
                features.set(featuresArray);
            }
            else if (features.get().length > offset) {
                System.err.println(
                        "ERROR: number of features greater than offset");
                System.exit(1);
            }
            ArrayWritable mergedFeatures =
                    mergeFeatures(features, null, offset);
            res.add(new PairWritable3(target, mergedFeatures));
            i++;
        }
        while (j < targetsAndFeatures2Array.length) {
            RuleWritable target2 =
                    ((PairWritable3) targetsAndFeatures2Array[j]).first;
            ArrayWritable features2 =
                    ((PairWritable3) targetsAndFeatures2Array[j]).second;
            ArrayWritable mergedFeatures =
                    mergeFeatures(null, features2, offset);
            res.add(new PairWritable3(target2, mergedFeatures));
            j++;
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
        // HFile.Writer writer = new HFile.Writer(fs, path);
        HFile.Writer writer = new HFile.Writer(fs, path, 16 * 1024, "gz", null);
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
        System.err.println(bytes2RuleWritable(toBeProcessed.get(0).getFirst()));
        System.err.println(bytes2RuleWritable(toBeProcessed.get(1).getFirst()));
        RuleWritable r1 = bytes2RuleWritable(toBeProcessed.get(0).getFirst());
        RuleWritable r2 = bytes2RuleWritable(toBeProcessed.get(1).getFirst());
        System.exit(1);
        boolean allHFilesEmpty = false;
        boolean isnonext;
        // determine if we should scan the next element. initialize to false.
        // the first element will never be true
        boolean[] nonext = new boolean[scanners.size()];
        ByteBuffer previousMinSource = null;
        ByteBuffer minSource = null;
        RawComparator<byte[]> comparator = new ByteArrayComparator();
        while (!allHFilesEmpty) {
            previousMinSource = minSource;
            // process toBeProcessed first
            // find the minimum element
            int minIndex = 0;
            Pair<ByteBuffer, ArrayWritable> minElement =
                    toBeProcessed.get(0);
            minSource = minElement.getFirst();
            for (int i = 1; i < toBeProcessed.size(); i++) {
                // int cmp =
                // minSource.compareTo(toBeProcessed.get(i).getFirst());
                int cmp =
                        comparator.compare(minSource.array(), toBeProcessed
                                .get(i).getFirst().array());
                if (cmp > 0) {
                    minSource = toBeProcessed.get(i).getFirst();
                    minIndex = i;
                }
            }
            List<PairWritable3> mergedList = new ArrayList<PairWritable3>();
            for (int i = 0; i < toBeProcessed.size(); i++) {
                // int cmp =
                // minSource.compareTo(toBeProcessed.get(i).getFirst());
                int cmp =
                        comparator.compare(minSource.array(), toBeProcessed
                                .get(i).getFirst().array());
                if (cmp == 0) {
                    nonext[i] = false;
                    mergedList =
                            merge(mergedList, toBeProcessed.get(i).getSecond(),
                                    i);
                }
                else if (cmp < 0) {
                    nonext[i] = true;
                }
                else {
                    System.err.println("ERROR: "
                            + bytes2RuleWritable(minSource)
                            + " is supposed to be smaller than "
                            + bytes2RuleWritable(toBeProcessed.get(i)
                                    .getFirst()));
                    System.exit(1);
                }
            }
            PairWritable3[] mergedArray =
                    mergedList.toArray(new PairWritable3[]{});
            ArrayWritable merged = new ArrayWritable(PairWritable3.class);
            merged.set(mergedArray);
            byte[] valueBytes = object2ByteArray(merged);
            try {
                writer.append(minSource.array(), valueBytes);
            }
            catch (IOException e) {
                e.printStackTrace();
                System.err.println("current source: "
                        + bytes2RuleWritable(minSource));
                System.err.println("previous source: "
                        + bytes2RuleWritable(previousMinSource));
                System.err.println(minSource.compareTo(previousMinSource));
                // RawComparator<byte[]> comparator = new ByteArrayComparator();
                System.err.println(comparator.compare(minSource.array(),
                        previousMinSource.array()));
                System.exit(1);
            }
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
