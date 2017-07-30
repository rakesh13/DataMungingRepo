package com.datamunging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedHashMap;
import java.util.Map;

public class Header {

public Map<String, Integer> getHeaderInfo(String filePath) {
		
		
		Map<String, Integer> headerInfo = new LinkedHashMap<>();
		try
		{
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String header = br.readLine();
		
		
		String []colInfo=header.split(",");
		int len=colInfo.length;
		for(int i=0;i<len;i++)
		{
			headerInfo.put(colInfo[i], i);
			
		}
		br.close();
		
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		
		return headerInfo;
	
	}
}
