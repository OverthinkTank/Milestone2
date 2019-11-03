/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cecs429.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author dhrum
 */
public class InvertedIndex implements Index {

    private HashMap<String, List<Posting>> map;

    public InvertedIndex() {
        map = new HashMap<String, List<Posting>>();
    }

    public void addTerm(String term, int docID, int position) {
        List<Integer> pos = new ArrayList<Integer>();
        pos.add(position);
        Posting doc = new Posting(docID, pos);
        List<Posting> list = new ArrayList<Posting>();
        if (map.containsKey(term)) {

            list = map.get(term);
            Posting lastdoc = list.get(list.size() - 1);
            if (lastdoc.getDocumentId() != doc.getDocumentId()) {
                list.add(doc);
                map.put(term, list);
            } else {
                List<Integer> postion_list = lastdoc.getPositionList();
                postion_list.add(position);
                lastdoc.setPostionList(postion_list);
                list.set(list.size() - 1, lastdoc);
                map.put(term, list);
            }

        } else {
            list.add(doc);
            map.put(term, list);
        }

    }

    public void addTerm(String term, int docID) {

        Posting doc = new Posting(docID);
        List<Posting> list = new ArrayList<Posting>();
        if (map.containsKey(term)) {

            list = map.get(term);
            Posting lastdoc = list.get(list.size() - 1);
            if (lastdoc.getDocumentId() != doc.getDocumentId()) {
                list.add(doc);
                map.put(term, list);
            }

        } else {
            list.add(doc);
            map.put(term, list);
        }

    }

    @Override
    public List<Posting> getPostings(String term) {
        if (map.containsKey(term)) {
            List<Posting> l = new ArrayList<>(map.get(term));
            return l;
        } else {
            List<Posting> l = new ArrayList<>();
            return l;
        }
    }

    @Override
    public List<String> getVocabulary() {
        List<String> s = new ArrayList<String>(map.keySet());

        Collections.sort(s);

        return s;
    }

}
