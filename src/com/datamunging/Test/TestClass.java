package com.datamunging.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datamunging.DataReader;
import com.datamunging.DataRow;
import com.datamunging.Query;
import com.datamunging.QueryParser;

import static org.junit.Assert.*;

public class TestClass {

	static QueryParser query;
	static DataReader dataFileReader;

	
	@BeforeClass
	public static void init()
	{
		query = new QueryParser();
		dataFileReader = new DataReader();
		
	}
	@Ignore
	@Test
     public void selectAllWithoutWhereTestCase() throws Exception{
		Query queryParameters=query.executeQuery("select Dept,sum(Salary) from D:\\DT Vsn3\\SampleData\\emp.csv where Salary>3000 group by Dept");
		List<DataRow> dataSet=dataFileReader.getData(queryParameters);
		assertNotNull(dataSet);
		display("selectAllWithoutWhereTestCase",dataSet);
		
	}
	@Ignore
	@Test
	public void selectColumnsWithoutWhereTestCase() throws Exception{
		
		Query queryParameters=query.executeQuery("select * from D:\\DT Vsn3\\SampleData\\emp.csv where Salary>3000");
		List<DataRow> dataSet=dataFileReader.getData(queryParameters);
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereGreaterThanTestCase() throws Exception{
		
		Query queryParameters=query.executeQuery("select City,Dept,Name from D:\\DT Vsn3\\SampleData\\emp.csv where Salary>30000");
		List<DataRow> dataSet=dataFileReader.getData(queryParameters);
		assertNotNull(dataSet);
		display("withWhereGreaterThanTestCase",dataSet);
		
	}
	@Ignore
	@Test
	public void withWhereLessThanTestCase() throws Exception{
		
		Query queryParameters=query.executeQuery("select City,Dept,Name from D:\\DT Vsn3\\SampleData\\emp.csv where Salary<3000");
		List<DataRow> dataSet=dataFileReader.getData(queryParameters);
		assertNotNull(dataSet);
		display("withWhereLessThanTestCase",dataSet);
		
	}
	@Ignore
	@Test
	public void withWhereLessThanOrEqualToTestCase() throws Exception{
		
		Query queryParameters=query.executeQuery("select City,Dept,Name from D:\\DT Vsn3\\SampleData\\emp.csv where Salary<=35000");
		List<DataRow> dataSet=dataFileReader.getData(queryParameters);
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase",dataSet);
		
	}
	@Ignore
	@Test
	public void withWhereGreaterThanOrEqualToTestCase() throws Exception{
		
		Query queryParameters=query.executeQuery("select City,Dept,Name from D:\\DT Vsn3\\SampleData\\emp.csv where Salary>=35000");
		List<DataRow> dataSet=dataFileReader.getData(queryParameters);
		assertNotNull(dataSet);
		display("withWhereGreaterThanOrEqualToTestCase",dataSet);
		
	}
	@Ignore
	@Test
	public void withWhereNotEqualToTestCase() throws Exception{
		
		Query queryParameters=query.executeQuery("select City,Dept,Name from D:\\DT Vsn3\\SampleData\\emp.csv where Salary != 35000");
		List<DataRow> dataSet=dataFileReader.getData(queryParameters);
		assertNotNull(dataSet);
		display("withWhereNotEqualToTestCase",dataSet);
		
	}
	@Ignore
	@Test
	public void withWhereEqualAndNotEqualTestCase() throws Exception{
		

		Query queryParameters=query.executeQuery("select City,Name,Salary from D:\\DT Vsn3\\SampleData\\emp.csv where Salary<=38000 and Salary>30000");
		List<DataRow> dataSet=dataFileReader.getData(queryParameters);
		assertNotNull(dataSet);
		display("withWhereEqualAndNotEqualTestCase",dataSet);
		
	}
	private void display(String testCaseName, List<DataRow> dataSet) {
		System.out.println(testCaseName);
		System.out.println("******************************************");
		
		for(Map<Integer, String> resultData:dataSet)
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
		System.out.println("******************************************");
	}
}
