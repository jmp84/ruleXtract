/**
 * 
 */

package uk.ac.cam.eng.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.berkeley.nlp.syntax.Tree;
import edu.berkeley.nlp.syntax.Trees;

/**
 * @author jmp84 Simple utility to read a file containing parse trees in Penn
 *         Treebank (PTB) format and convert the leaves to word ids.
 */
public class MapTreeToIds2 {

    /**
     * @param args
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        if (args.length != 3) {
            System.err.println("Args: <word map> <PTB tree file> <words>");
            System.exit(1);
        }
        Map<String, String> wordMap = new HashMap<String, String>();
        try (BufferedReader br =
                new BufferedReader(new FileReader(args[0]))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                wordMap.put(parts[1], parts[0]);
            }
        }
        try (BufferedReader br =
                new BufferedReader(new FileReader(args[1]));
                BufferedReader brWords =
                        new BufferedReader(new FileReader(args[2]))) {
            String line;
            String lineWords;
            while ((line = br.readLine()) != null
                    && (lineWords = brWords.readLine()) != null) {
                if (line.equals("(())")) {
                    continue; // empty parse
                }
                String[] words = lineWords.split("\\s+");
                Tree parseTree = Trees.PennTreeReader.parseEasy(line);
                Iterator<Tree> parseTreeIterator =
                        parseTree.iterator();
                int index = 0;
                while (parseTreeIterator.hasNext()) {
                    Tree next = parseTreeIterator.next();
                    if (next.isLeaf()) {
                        String word = (String) next.getLabel();
                        String wordCheck = words[index];
                        if (word.equals(wordCheck)) {
                            next.setLabel(wordMap.get(word));
                        }
                        // this means that the word was discarded in parsing and
                        // only the tag was left. I an empty tag was left, we
                        // set the tag to be 'X'
                        else {
                            // next.setLabel(wordMap.get(wordCheck));
                            List<Tree> children = new ArrayList<Tree>();
                            children.add(new Tree(wordMap.get(wordCheck)));
                            next.setChildren(children);
                            if (((String) next.getLabel()).isEmpty()) {
                                next.setLabel("X");
                            }
                        }
                        index++;
                    }
                }
                System.out.println(parseTree.toString());
            }
        }
    }
}
