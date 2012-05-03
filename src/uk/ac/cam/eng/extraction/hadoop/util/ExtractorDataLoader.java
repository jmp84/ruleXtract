
package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * Load all the rule alignment data onto HDFS ready to have rules extracted
 * 
 * @author aaw35
 */
public class ExtractorDataLoader {

    private static abstract class RecordReader {

        BufferedReader in;
        StringBuilder out = new StringBuilder();
        int sentence;
        Set<String> provenance = new HashSet<>();

        void setFileName(String fileName) throws FileNotFoundException,
                IOException {
            in = new BufferedReader(new InputStreamReader(new GZIPInputStream(
                    new FileInputStream(fileName))));
        }

        abstract String getNext() throws IOException;
    }

    /**
     * @param args
     * @throws IOException
     */
    public void acn2hadoop(String sentenceAlignmentFile,
            String wordAlignmentFile, String hdfsName) throws IOException {
        RecordReader sentenceReader = new RecordReader() {

            @Override
            String getNext() throws IOException {
                String line = in.readLine();
                if (line == null) {
                    return null;
                }
                sentence = Integer.parseInt(line);
                out.setLength(0);
                out.append(in.readLine()).append("\n");
                out.append(in.readLine()).append("\n");
                return out.toString();
            }
        };
        sentenceReader.setFileName(sentenceAlignmentFile);
        RecordReader alignmentReader = new RecordReader() {

            String prevLine = null;

            @Override
            String getNext() throws IOException {
                if (prevLine == null) {
                    prevLine = in.readLine();
                }
                if (!prevLine.startsWith("SENT: ")) {
                    throw new RuntimeException(
                            "Error in the word alignment file");
                }
                String[] parts = prevLine.split("\\s+");
                sentence = Integer.parseInt(parts[1]);
                // clear provenance beforehand
                provenance.clear();
                for (int i = 2; i < parts.length; i++) {
                    provenance.add(parts[i]);
                }
                out.setLength(0);
                String line = in.readLine();
                if (line == null) {
                    return null;
                }
                while (line != null) {
                    if (line.startsWith("S ")) {
                        out.append(line).append("\n");
                    }
                    else {
                        prevLine = line;
                        break;
                    }
                    line = in.readLine();
                }
                return out.toString();
            }

        };
        alignmentReader.setFileName(wordAlignmentFile);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(hdfsName);

        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
                MapWritable.class, TextArrayWritable.class);
        try {
            Text sentenceText = new Text();
            Text alignmentText = new Text();
            Text[] array = new Text[2];
            array[0] = sentenceText;
            array[1] = alignmentText;
            TextArrayWritable arrayWritable = new TextArrayWritable();
            Text sentenceNumber = new Text();
            // metadata: provenance, e.g. genre, collection, training instance
            // id, etc.
            MapWritable metadata = new MapWritable();
            int sentenceNumberInt = 0;
            String sentence = sentenceReader.getNext();
            String alignment = alignmentReader.getNext();
            while (sentence != null || alignment != null) {
                // we ignore the sentence number given by the input file
                // because it is not reliable (data splitting)
                // sentenceNumber.set(alignmentReader.sentence);
                sentenceNumber.set(Integer.toString(sentenceNumberInt));
                metadata.clear();
                metadata.put(sentenceNumber, NullWritable.get());
                for (String prov: alignmentReader.provenance) {
                    metadata.put(new Text(prov), NullWritable.get());
                }
                sentenceNumberInt++;
                sentenceText.set(sentence);
                // handle empty alignment case
                if (alignment == null) {
                    alignmentText.set("");
                }
                else {
                    alignmentText.set(alignment);
                }
                arrayWritable.set(array);
                writer.append(metadata, arrayWritable);
                sentence = sentenceReader.getNext();
                alignment = alignmentReader.getNext();
            }
        }
        finally {
            writer.close();
        }
    }
}
