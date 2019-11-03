/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import java.util.List;

/**
 *
 * @author dhrum
 */
public class NotQuery implements QueryComponent {

    private QueryComponent mComponent;

    public NotQuery(QueryComponent component) {
        mComponent = component;
    }

    @Override
    public List<Posting> getPostings(Index[] index) {
        return mComponent.getPostings(index);
    }

    @Override
    public boolean gettype() {
        return false;
    }

}
