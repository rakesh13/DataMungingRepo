package com.datamunging;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DataMungingMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			QueryParser queryParser = new QueryParser();
			String query = "select count(Age) from D:\\DT Vsn3\\SampleData\\emp.csv";
			Query queryParameters = queryParser.executeQuery(query);

			DataReader dataReader = new DataReader();

			List<DataRow> rowSetData = dataReader.getData(queryParameters);
			/*Map<Integer,String> list=new LinkedHashMap<>();
			for (Map.Entry<Integer, Map<Integer,String>> resultSet : rowSetData.entrySet()) {

				Map<Integer,String> resultData=resultSet.getValue();
				//System.out.println(resultData);
				
				if(!resultSet.getValue().isEmpty())
				{
					for(Map.Entry<Integer, String> result:resultData.entrySet())
					{
						System.out.print(result.getValue()+"  ");
						//list.add(result.getValue());
						//list.put(result.getKey(), result.getValue());
					}
					System.out.print("\n");
				}
				
			}*/
			for(Map<Integer, String> resultData:rowSetData)
			{
				if(!resultData.isEmpty())
				{
					for(Map.Entry<Integer, String> result:resultData.entrySet())
					{
						System.out.print(result.getValue()+"  ");
					}
					System.out.print("\n");
				}
			}
			
		}
		catch(Exception ex)
		{
			//System.out.println(ex.printStackTrace());
			ex.printStackTrace();
		}
		
	}

}
