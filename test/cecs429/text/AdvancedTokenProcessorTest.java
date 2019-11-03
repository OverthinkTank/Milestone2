/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cecs429.text;

import cecs429.documents.DirectoryCorpus;
import cecs429.documents.Document;
import cecs429.documents.DocumentCorpus;
import cecs429.index.Index;
import cecs429.index.InvertedIndex;
import cecs429.index.Posting;
import cecs429.query.BooleanQueryParser;
import cecs429.query.QueryComponent;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Dipesh
 */
public class AdvancedTokenProcessorTest {

    public AdvancedTokenProcessorTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of processToken method, of class AdvancedTokenProcessor.
     */
    @Test
    public void testProcessToken() throws IOException {
        System.out.println("Advance Token Processor Case I Complete");
        DocumentCorpus corpus = DirectoryCorpus.loadJsonDirectory(Paths.get("C:\\Users\\dipes\\Desktop\\Unit Test\\JSON").toAbsolutePath(), ".json");
        String query = "(256.256.256.256)";    //And Query with Phrase Query..
        Index[] index = indexCorpus(corpus);
        BooleanQueryParser queryparser = new BooleanQueryParser();
        QueryComponent query_component = queryparser.parseQuery(query);
        int i = 0;
        List<Posting> result_docs = query_component.getPostings(index);
        String expResult = "5";
        String results = "";
        for (Posting p : result_docs) {
            results = results + (p.getDocumentId() + 1);
        }
        assertEquals(expResult.trim(), results.trim());
    }

    @Test
    public void testProcessTokenTwo() throws IOException {
        System.out.println("Advance Token Processor Case II Complete");
        DocumentCorpus corpus = DirectoryCorpus.loadJsonDirectory(Paths.get("C:\\Users\\dipes\\Desktop\\Unit Test\\JSON").toAbsolutePath(), ".json");
        String query = "Hello";    //And Query with Phrase Query..
        Index[] index = indexCorpus(corpus);
        BooleanQueryParser queryparser = new BooleanQueryParser();
        QueryComponent query_component = queryparser.parseQuery(query);
        int i = 0;
        List<Posting> result_docs = query_component.getPostings(index);
        String expResult = "1";
        String results = "";
        for (Posting p : result_docs) {
            results = results + (p.getDocumentId() + 1);
        }
        assertEquals(expResult.trim(), results.trim());
    }

    @Test
    public void testProcessTokenThree() throws IOException {
        System.out.println("Advance Token Processor Case III Complete");
        DocumentCorpus corpus = DirectoryCorpus.loadJsonDirectory(Paths.get("C:\\Users\\dipes\\Desktop\\Unit Test\\JSON").toAbsolutePath(), ".json");
        String query = "firstDocument";    //And Query with Phrase Query..
        Index[] index = indexCorpus(corpus);
        BooleanQueryParser queryparser = new BooleanQueryParser();
        QueryComponent query_component = queryparser.parseQuery(query);
        int i = 0;
        List<Posting> result_docs = query_component.getPostings(index);
        String expResult = "1";
        String results = "";
        for (Posting p : result_docs) {
            results = results + (p.getDocumentId() + 1);
        }
        assertEquals(expResult.trim(), results.trim());
    }

    @Test
    public void testProcessTokenFour() throws IOException {
        System.out.println("Advance Token Processor Case IV Complete");
        DocumentCorpus corpus = DirectoryCorpus.loadJsonDirectory(Paths.get("C:\\Users\\dipes\\Desktop\\Unit Test\\JSON").toAbsolutePath(), ".json");
        String query = "\"this\"";    //And Query with Phrase Query..
        Index[] index = indexCorpus(corpus);
        BooleanQueryParser queryparser = new BooleanQueryParser();
        QueryComponent query_component = queryparser.parseQuery(query);
        int i = 0;
        List<Posting> result_docs = query_component.getPostings(index);
        String expResult = "135";
        String results = "";
        for (Posting p : result_docs) {
            results = results + (p.getDocumentId() + 1);
        }
        assertEquals(expResult.trim(), results.trim());
    }

    @Test
    public void testProcessTokenFive() throws IOException {
        System.out.println("Advance Token Processor Case IV Complete");
        DocumentCorpus corpus = DirectoryCorpus.loadJsonDirectory(Paths.get("C:\\Users\\dipes\\Desktop\\Unit Test\\JSON").toAbsolutePath(), ".json");
        String query = "It\'s";    //And Query with Phrase Query..
        Index[] index = indexCorpus(corpus);
        BooleanQueryParser queryparser = new BooleanQueryParser();
        QueryComponent query_component = queryparser.parseQuery(query);
        int i = 0;
        List<Posting> result_docs = query_component.getPostings(index);
        String expResult = "1";
        String results = "";
        for (Posting p : result_docs) {
            results = results + (p.getDocumentId() + 1);
        }
        assertEquals(expResult.trim(), results.trim());
    }

    private static Index[] indexCorpus(DocumentCorpus corpus) throws IOException {
        HashSet<String> vocabulary = new HashSet<>();
        AdvancedTokenProcessor processor = new AdvancedTokenProcessor();
        InvertedIndex index = new InvertedIndex();
        InvertedIndex biword = new InvertedIndex();
        for (Document d : corpus.getDocuments()) {
            EnglishTokenStream ets = new EnglishTokenStream(d.getContent());
            int term_position = 0;
            String previous = "";
            for (String s : ets.getTokens()) {
                term_position++;
                List<String> word = processor.processToken(s);
                for (int i = 0; i < word.size(); i++) {
                    index.addTerm(word.get(i), d.getId(), term_position);
                    if (previous != "") {
                        biword.addTerm(previous + " " + word.get(i), d.getId());
                    }
                    previous = word.get(i);
                }
            }
            ets.close();
        }
        InvertedIndex[] i = {index, biword};
        return i;
    }
}
