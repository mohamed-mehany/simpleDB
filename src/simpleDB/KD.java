package simpleDB;

import java.io.Serializable;

import simpleDB.kdtree.KDTree;

@SuppressWarnings("serial")
public class KD extends KDTree implements Serializable {

	public KD(int k) {
		super(k);
	}
	public KD(){
		super(0);
	}

}