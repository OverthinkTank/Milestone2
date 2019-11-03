/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package disk;

import cecs429.index.Index;
import cecs429.index.Posting;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 * @author dhrum
 */
public class DiskIndexWriter {

String gethex(int hex)
{
    String s_hex = Integer.toHexString(hex);
    String s ="";
    for(int i=1;i<=8-s_hex.length();i++)
    {
        s += "0";
    }
    s_hex = s+s_hex;
    return s_hex;
}
public List<Long> write_posting(Index index, String path) throws IOException
{
    List<String> vocab = index.getVocabulary();
    List<Long> voc_pos= new ArrayList();
    //DataOutputStream writer = new DataOutputStream(new FileOutputStream(path+"posting.bin"));
    File yourFile = new File(path+"postings.bin");
    yourFile.getParentFile().mkdirs();
    yourFile.createNewFile(); 
    RandomAccessFile writer = new RandomAccessFile(yourFile, "rw");
    Long vocab_position;
    int previous_doc = 0;
    for(String str : vocab)
    {
        List<Posting> postings = index.getPostings(str);
        int dft = postings.size();
        byte[] hex_dft = ByteBuffer.allocate(4).putInt(dft).array();
        
        //writer.writeBytes(dft);
        vocab_position = writer.getFilePointer();
        voc_pos.add(vocab_position);
        writer.write(hex_dft,0,hex_dft.length);
        previous_doc = 0;
        int id =0;
        for(Posting p: postings)
        {
            id = p.getDocumentId();
            id = id - previous_doc;
            previous_doc = id;
            byte[] hex_id = ByteBuffer.allocate(4).putInt(id).array();
            writer.write(hex_id,0, hex_id.length);
            List<Integer> positions = p.getPositionList();
            int tftd = positions.size();
            byte[] hex_tftd =  ByteBuffer.allocate(4).putInt(tftd).array();
            writer.write(hex_tftd,0, hex_tftd.length);
            int previous_position = 0;
            for(int position:positions)
            {
                position = position - previous_position;
                previous_position = position;
                byte[] hex_position =  ByteBuffer.allocate(4).putInt(position).array();
                writer.write(hex_position,0,hex_position.length);
            }
        }
    }
    writer.close();

    return voc_pos;
}

public List<Long> write_vocab(List<String> vocab, String path) throws IOException
{
    
    List<Long> positions= new ArrayList();
    File yourFile = new File(path+"vocab.bin");
    yourFile.getParentFile().mkdirs();
    yourFile.createNewFile(); 
    RandomAccessFile raf = new RandomAccessFile(yourFile, "rw");
    
    Long pointer;
    for(String str : vocab)
    {
        pointer = raf.getFilePointer();
        raf.writeBytes(str);
        positions.add(pointer);
        //pointer += str.length();
    }
    raf.close();
    return positions;
}
public void write_vocab_table(List<Long> vocab, List<Long> posting, String path) throws FileNotFoundException, IOException
{
    File yourFile = new File(path+"vocabTable.bin");
    yourFile.getParentFile().mkdirs();
    yourFile.createNewFile(); 
    
    RandomAccessFile voc_table = new RandomAccessFile(yourFile, "rw");
   // System.out.println(vocab.size()+" "+posting.size());
  // byte[] size = ByteBuffer.allocate(4).putInt(vocab.size()).array();
  // voc_table.write(size, 0, size.length);
    for(int i= 0; i<vocab.size();i++)
    {
        byte[] v = ByteBuffer.allocate(8).putLong(vocab.get(i)).array();
        
        voc_table.write(v, 0, v.length);
        byte[] v1 = ByteBuffer.allocate(8).putLong(posting.get(i)).array();
        voc_table.write(v1, 0, v1.length);
        
        
    }
    
    
}
}
