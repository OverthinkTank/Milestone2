package cecs429.documents;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.gson.Gson;

public class JsonFileDocument implements FileDocument {

    int mDocumentId;
    private Path mFilePath;
    private AllSites mDoc;

    /*
	 * Gson gson = new Gson(); JsonReader j = new JsonReader();
     */
    public JsonFileDocument(int id, Path absoluteFilePath) {
        mDocumentId = id;
        mFilePath = absoluteFilePath;

        String contents;
        try {
            contents = readFile(absoluteFilePath.toString(), StandardCharsets.US_ASCII);

            Gson gson = new Gson();
            mDoc = gson.fromJson(contents, AllSites.class);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public int getId() {
        // TODO Auto-generated method stub
        return mDocumentId;

    }

    public Reader getContent() {

        System.out.println(mFilePath);
        return new StringReader(mDoc.body);
    }

    @Override
    public String getTitle() {
        return mDoc.title;
    }

    @Override
    public Path getFilePath() {
        // TODO Auto-generated method stub
        return mFilePath;
    }

    public static FileDocument loadJsonFileDocument(Path absolutePath, int documentId) {
        return new JsonFileDocument(documentId, absolutePath);

    }

}
