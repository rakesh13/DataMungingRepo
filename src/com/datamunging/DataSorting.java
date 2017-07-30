package com.datamunging;

import java.util.Comparator;

public class DataSorting implements Comparator<DataRow>
{

	private int sortingIndex;
	
	public int getSortingIndex() {
		return sortingIndex;
	}

	public void setSortingIndex(int sortingIndex) {
		this.sortingIndex = sortingIndex;
		
	}

	@Override
	public int compare(DataRow list1, DataRow list2) {
		// TODO Auto-generated method stub
		return list1.get(sortingIndex).compareTo(list2.get(sortingIndex));
	}

	


}
