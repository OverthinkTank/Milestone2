package disk;


import java.util.*;




import cecs429.index.Index;
import cecs429.index.Posting;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import java.util.*;




import cecs429.index.Index;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;
public class DiskInvertedIndex implements Index {

   private String mPath;
   private RandomAccessFile mVocabList;
   private RandomAccessFile mPostings;
   private long[] mVocabTable;

   // Opens a disk inverted index that was constructed in the given path.
   public DiskInvertedIndex(String path) {
      try {
         mPath = path;
         mVocabList = new RandomAccessFile(new File(path, "vocab.bin"), "r");
         mPostings = new RandomAccessFile(new File(path, "postings.bin"), "r");
         mVocabTable = readVocabTable(path);
         //mFileNames = readFileNames(path);
      }
      catch (FileNotFoundException ex) {
         System.out.println(ex.toString());
      }
   }

  

   // Locates the byte position of the postings for the given term.
   // For example, binarySearchVocabulary("angel") will return the byte position
   // to seek to in postings.bin to find the postings for "angel".
   private long binarySearchVocabulary(String term) {
      // do a binary search over the vocabulary, using the vocabTable and the file vocabList.
      int i = 0, j = mVocabTable.length / 2 - 1;
      while (i <= j) {
         try {
            int m = (i + j) / 2;
            long vListPosition = mVocabTable[m * 2];
            int termLength;
            if (m == mVocabTable.length / 2 - 1) {
               termLength = (int)(mVocabList.length() - mVocabTable[m*2]);
            }
            else {
               termLength = (int) (mVocabTable[(m + 1) * 2] - vListPosition);
            }

            mVocabList.seek(vListPosition);

            byte[] buffer = new byte[termLength];
            mVocabList.read(buffer, 0, termLength);
            String fileTerm = new String(buffer, "ASCII");

            int compareValue = term.compareTo(fileTerm);
            if (compareValue == 0) {
               System.out.print("found it!");
               return mVocabTable[m * 2 + 1];
            }
            else if (compareValue < 0) {
               j = m - 1;
            }
            else {
               i = m + 1;
            }
         }
         catch (IOException ex) {
            System.out.println(ex.toString());
         }
      }
      return -1;
   }

   // Reads the file vocabTable.bin into memory.
   private static long[] readVocabTable(String indexName) {
      try {
         long[] vocabTable;
         
         RandomAccessFile tableFile = new RandomAccessFile(
          new File(indexName, "vocabTable.bin"),
          "r");
         
         //byte[] byteBuffer = new byte[4];
         
         //tableFile.read(byteBuffer, 0, byteBuffer.length);
         
         int tableIndex = 0;
        // System.out.println((int)tableFile.length()/16*2);
         vocabTable = new long[3632];
         byte[] byteBuffer = new byte[8];
         //vocabTable[0] = 9;
         
         
         while (tableFile.read(byteBuffer, 0, byteBuffer.length) > 0) { // while we keep reading 4 bytes
            
            vocabTable[tableIndex] = ByteBuffer.wrap(byteBuffer).getLong();
            tableIndex++;
         }
         tableFile.close();
         return vocabTable;
      }
      catch (FileNotFoundException ex) {
         System.out.println(ex.toString());
      }
      catch (IOException ex) {
         System.out.println(ex.toString());
      }
      return null;
   }

   public int getTermCount() {
      return mVocabTable.length / 2;
   }

    @Override
    public List<Posting> getPostings(String term) {
    
    
    long term_position = binarySearchVocabulary(term);
    
       try {
           mPostings.seek(term_position);
          System.out.println("Position"+term_position);
       } catch (IOException ex) {
           Logger.getLogger(DiskInvertedIndex.class.getName()).log(Level.SEVERE, null, ex);
       }
    
    List<Posting> posting_list = new ArrayList<>();
    byte[] byteBuffer = new byte[4];
       try {
           mPostings.read(byteBuffer, 0, byteBuffer.length);
           ByteBuffer wrapped = ByteBuffer.wrap(byteBuffer);
           int dft = wrapped.getInt();
           int prev = 0;
           int doc_id;
           for(int i=0;i<dft;i++)
           {
               mPostings.read(byteBuffer, 0, byteBuffer.length);
              //System.out.println(ByteBuffer.wrap(byteBuffer).getInt());
               doc_id = prev + ByteBuffer.wrap(byteBuffer).getInt();
               prev = ByteBuffer.wrap(byteBuffer).getInt();
               //doc_id =   ByteBuffer.wrap(byteBuffer).getInt();
            // System.out.println(doc_id);
               mPostings.read(byteBuffer, 0, byteBuffer.length);
               //System.out.println(ByteBuffer.wrap(byteBuffer).getInt());
               int tftd = ByteBuffer.wrap(byteBuffer).getInt();
               int position = 0;
               List<Integer> position_list = new ArrayList();
               for(int j = 0; j<tftd ;j++)
               {
                   mPostings.read(byteBuffer, 0, byteBuffer.length);
                  // System.out.println(ByteBuffer.wrap(byteBuffer).getInt());
                   position = position + ByteBuffer.wrap(byteBuffer).getInt();
                   position_list.add(position);
               }
              // System.out.println();
               Posting p = new Posting(doc_id, position_list);
           posting_list.add(p);    
           }
           
       } catch (IOException ex) {
           Logger.getLogger(DiskInvertedIndex.class.getName()).log(Level.SEVERE, null, ex);
       }
     
     return posting_list;
    }

    @Override
    public List<String> getVocabulary() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
