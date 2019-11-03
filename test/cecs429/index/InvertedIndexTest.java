/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cecs429.index;

import cecs429.documents.DirectoryCorpus;
import cecs429.documents.Document;
import cecs429.documents.DocumentCorpus;
import cecs429.query.BooleanQueryParser;
import cecs429.query.QueryComponent;
import cecs429.text.AdvancedTokenProcessor;
import cecs429.text.EnglishTokenStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import static jdk.nashorn.internal.objects.NativeArray.map;
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
public class InvertedIndexTest {

    public InvertedIndexTest() {
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

    @Test
    public void testGetVocabulary() throws IOException {

        System.out.println("Inverted Index Test Case I Complete");
        DocumentCorpus corpus = DirectoryCorpus.loadJsonDirectory(Paths.get("C:\\Users\\dipes\\Desktop\\Unit Test\\JSON").toAbsolutePath(), ".json");
        String query = "256.256.256.256";
        Index[] index = indexCorpus(corpus);
        BooleanQueryParser queryparser = new BooleanQueryParser();
        QueryComponent query_component = queryparser.parseQuery(query);
        List<Posting> result_docs = query_component.getPostings(index);
        String expResult = "5[4]";
        String results = "";
        for (Posting p : result_docs) {
            results = results + (p.getDocumentId() + 1) + (p.getPositionList());
        }
        assertEquals(expResult.trim(), results.trim());
    }

    @Test
    public void testGetVocabularyTwo() throws IOException {
        System.out.println("Inverted Index Test Case II Complete");
        DocumentCorpus corpus = DirectoryCorpus.loadJsonDirectory(Paths.get("C:\\Users\\dipes\\Desktop\\Unit Test\\JSON").toAbsolutePath(), ".json");
        String query = "firstdocument";
        Index[] index = indexCorpus(corpus);
        BooleanQueryParser queryparser = new BooleanQueryParser();
        QueryComponent query_component = queryparser.parseQuery(query);
        List<Posting> result_docs = query_component.getPostings(index);
        String expResult = "1[5]";
        String results = "";
        for (Posting p : result_docs) {
            results = results + (p.getDocumentId() + 1) + (p.getPositionList());
        }
        assertEquals(expResult.trim(), results.trim());
    }

    @Test
    public void testGetVocabularyThree() throws IOException {
        System.out.println("Inverted Index Test Case III Complete");
        DocumentCorpus corpus = DirectoryCorpus.loadJsonDirectory(Paths.get("C:\\Users\\dipes\\Desktop\\Unit Test\\JSON").toAbsolutePath(), ".json");
        String query = "JSON";
        Index[] index = indexCorpus(corpus);
        BooleanQueryParser queryparser = new BooleanQueryParser();
        QueryComponent query_component = queryparser.parseQuery(query);
        List<Posting> result_docs = query_component.getPostings(index);
        String expResult = "2[3]";
        String results = "";
        for (Posting p : result_docs) {
            results = results + (p.getDocumentId() + 1) + p.getPositionList();
        }
        assertEquals(expResult.trim(), results.trim());
    }

    @Test
    public void testGetVocabularyFour() throws IOException {

        System.out.println("Inverted Test Case IV Complete");
        DocumentCorpus corpus = DirectoryCorpus.loadJsonDirectory(Paths.get("C:\\Users\\dipes\\Desktop\\Unit Test\\JSON").toAbsolutePath(), ".json");
        String query = "Testing";
        Index[] index = indexCorpus(corpus);

        BooleanQueryParser queryparser = new BooleanQueryParser();
        QueryComponent query_component = queryparser.parseQuery(query);
        int i = 0;
        List<Posting> result_docs = query_component.getPostings(index);
        String expResult = "12";
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
