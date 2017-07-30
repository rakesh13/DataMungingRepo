package com.datamunging;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.crypto.Data;

public class DataReader {

	private Map<String, Integer> headerKeyValue;
	private List<Integer> headerIndex;
	private List<String> headerColumn;
	private Header header;
	private BufferedReader bufferedReader;
	private DataRow rowSetData;
	private List<DataRow> csvFileData;
	private int value;
	private Query queryParameters;
	
	public List<DataRow> getData(Query queryParameters) throws Exception {
		this.queryParameters=queryParameters;
	
		csvFileData = new ArrayList<>();
		bufferedReader = new BufferedReader(new FileReader(queryParameters.getFilePath()));
		try {

			bufferedReader.readLine();
			 

			header = new Header();
			headerKeyValue = header.getHeaderInfo(queryParameters.getFilePath());
			headerIndex = new ArrayList<>();
			headerColumn = new ArrayList<>();

			for (Map.Entry<String, Integer> mapKeyValue : headerKeyValue.entrySet()) {
				headerColumn.add(mapKeyValue.getKey());
				headerIndex.add(mapKeyValue.getValue());
			}
			
			
			
			if (queryParameters.isAllColumns() && queryParameters.getWhereCondition()==false) {

				String line;

				int rowCount = 0;
				
				while ((line = bufferedReader.readLine()) != null) {
					rowSetData = new DataRow();
					String[] colInfo = line.split(",");
					int x=0;
					for (String rowData : colInfo) {
						rowSetData.put(x,rowData);
						x++;
					}
					if(!rowSetData.isEmpty())
					csvFileData.add(rowSetData);
					rowCount++;
				}

			}
			else if(!queryParameters.isAllColumns()&& queryParameters.getFields().size()==1 && queryParameters.getGroupBy()==null)
			{
				int colIndex = 0;
				String colName,agregateFunction;
				List<String> allColumns=queryParameters.getFields();
				rowSetData=new DataRow();
				for(String str:allColumns)
				{
					String[] string=str.split("\\(");
					agregateFunction=string[0];
					String[] split1=string[1].split("\\)");
					colName=split1[0];
					for (String headerCol : headerColumn) {
						if(headerCol.equals(colName))
						{
							colIndex=headerColumn.indexOf(colName);
						}
					}
					int count=0;
					if(agregateFunction.equals("count"))
					{
						while(bufferedReader.readLine()!=null)
						{
							count++;
							
						}
						rowSetData.put(0, String.valueOf(count));
						csvFileData.add(rowSetData);
					}
					else if(agregateFunction.equals("sum"))
					{
						int sum=0;
					
						for (int headerIndexs : headerIndex) 
						{
							if (colIndex == headerIndexs) 
							{
								String line;
								while ((line = bufferedReader.readLine()) != null) 
								{

									String[] colInfo = line.split(",");
									int val=Integer.parseInt(colInfo[colIndex]);
									sum+=val;
								}
							}
						}
						rowSetData.put(0, String.valueOf(sum));
						csvFileData.add(rowSetData);
					}
					else if(agregateFunction.equals("avg"))
					{
						int sum=0,avg=0,total=0;
						
						for (int headerIndexs : headerIndex) 
						{
							if (colIndex == headerIndexs) 
							{
								String line;
								while ((line = bufferedReader.readLine()) != null) 
								{

									String[] colInfo = line.split(",");
									int val=Integer.parseInt(colInfo[colIndex]);
									sum+=val;
									total++;
								}
							}
						}
						avg=sum/total;
						rowSetData.put(0, String.valueOf(avg));
						csvFileData.add(rowSetData);
					}
					else if(agregateFunction.equals("max"))
					{
						int max=0,temp=0;
		            	 for (int headerIndexs : headerIndex) 
		                  {
		                      if (colIndex == headerIndexs) 
		                      {
		                          String line;
		                          while ((line = bufferedReader.readLine()) != null) 
		                          {

		                              String[] colInfo = line.split(",");
		                              int val=Integer.parseInt(colInfo[colIndex]);
		                              temp=val;
		                              if(max>temp)
		                              {
		                            	 max=temp;
		                              }
		                          }
		                      }
		                  }
		            	 rowSetData.put(0, String.valueOf(max));
		            	 csvFileData.add(rowSetData);
					}
				}
				
			}
			else if(!queryParameters.isAllColumns()&& queryParameters.getGroupBy()!=null)
			{
				int agregatecolIndex = 0;
				int colIndex=0;
				String colName,agregateFunction = null;
				String []colValue = new String[8];
				int [] agregateColValue = new int[8];
				List<String> queryFields=queryParameters.getFields();
				for(String colmn:queryFields)
				{
					if(colmn.contains("("))
					{
						String[] string=colmn.split("\\(");
						agregateFunction=string[0];
						String[] split1=string[1].split("\\)");
						colName=split1[0];
						for (String headerCol : headerColumn) {
							if(headerCol.equals(colName))
							{
								agregatecolIndex=headerColumn.indexOf(colName);
								
							}
						}
					}
					else
					{
						for (String headerCol : headerColumn) {
							if(headerCol.equals(colmn))
							{
								colIndex=headerColumn.indexOf(colmn);
								
							}
						}
					}
							
				}
				for (int headerIndexs : headerIndex) 
                {
                    if (colIndex == headerIndexs) 
                    {
                        String line;
                        int i=0;
                        while ((line = bufferedReader.readLine()) != null) 
                        {

                            String[] colInfo = line.split(",");
                            colValue[i]=colInfo[colIndex];
                            agregateColValue[i]=Integer.parseInt(colInfo[agregatecolIndex]);
                            i++;
                        }
                    }
                }
				
				int length=colValue.length;
				int sum;
				rowSetData=new DataRow();
				
				if(agregateFunction.equals("sum"))
				{
					int counter=-1;
				for(int i=0;i<length;i++)
				{
					String temp=colValue[i];
					sum=0;
					
					for(int j=0;j<length;j++)
					{
						if(colValue[j].equals(temp))
						{
							sum=sum+agregateColValue[j];
							
						}
					}
					rowSetData.put(counter++, colValue[i]);
					rowSetData.put(counter++, String.valueOf(sum));
				}
				}
				else if(agregateFunction.equals("count"))
				{
					int counter=-1;
					int count;
					for(int i=0;i<length;i++)
					{
						String temp=colValue[i];
						count=0;
						
						for(int j=0;j<length;j++)
						{
							if(colValue[j].equals(temp))
							{
								count=count+1;
								
							}
						}
						rowSetData.put(counter++, colValue[i]);
						rowSetData.put(counter++, String.valueOf(count));
					}
				}
				else if(agregateFunction.equals("avg"))
				{
					int counter=-1;
					int avg=0,count;
					for(int i=0;i<length;i++)
					{
						String temp=colValue[i];
						sum=0;
						count=0;
						for(int j=0;j<length;j++)
						{
							if(colValue[j].equals(temp))
							{
								sum=sum+agregateColValue[j];
								count=count+1;
							}
						}
						avg=sum/count;
						rowSetData.put(counter++, colValue[i]);
						rowSetData.put(counter++, String.valueOf(avg));
					}
				}
				
				csvFileData.add(rowSetData);
			}
			else if(queryParameters.isAllColumns() && queryParameters.getWhereCondition()==true)
			{
				
				List<Integer> selectedColumnIndex = new ArrayList<>();
				List<Criteria> restrictions = queryParameters.getRestrictions();
				List<Integer> indexOfConditionColumn = new ArrayList<>();
				String operator=queryParameters.getRestrictions().get(0).getOperator();
				if(!operator.equals("="))
				value=Integer.parseInt(queryParameters.getRestrictions().get(0).getValue());
				for (String headerCol : headerColumn) {
					
					selectedColumnIndex.add(headerColumn.indexOf(headerCol));
				}
				
				if (queryParameters.getLogicalOperator() == null) {
					for (String headerCol : headerColumn) {
						for (Criteria conditionColumns : restrictions) {
							if (headerCol.equals(conditionColumns.getColumnName())) {
								indexOfConditionColumn.add(headerColumn.indexOf(headerCol));
							}
						}
					}
					 switch (operator) {
						case ">":
							this.greaterThanOperation(selectedColumnIndex,indexOfConditionColumn,restrictions);
							break;
						case "<":
							this.lessThanOperation(selectedColumnIndex,indexOfConditionColumn,restrictions);
							break;
						case "=":
							this.equalOperation(selectedColumnIndex,indexOfConditionColumn,restrictions);
							break;
						case "<=":
							this.lessThanEqual(selectedColumnIndex,indexOfConditionColumn,restrictions);
						default:
							break;
						}
					}
				else
				{
					int totalLogicalOperator=queryParameters.getLogicalOperator().size();
					do
					{
						
					}while(totalLogicalOperator>0);
				}
			}
			else 
			{
				List<Integer> selectedColumnIndex = new ArrayList<>();
				List<String> selectedColumnNames = queryParameters.getFields();

				for (String headerCol : headerColumn) {
					for (String selectedColumns : selectedColumnNames) {
						if (headerCol.equals(selectedColumns)) {
							selectedColumnIndex.add(headerColumn.indexOf(headerCol));
						}
					}

				}
				
				if (queryParameters.getWhereCondition() == false) 
				{
					
					int resultIndex = 0;
					for (int headerIndexs : headerIndex) 
					{
						for (int columnIndex : selectedColumnIndex) 
						{
							if (columnIndex == headerIndexs) 
							{
								String line;
								while ((line = bufferedReader.readLine()) != null) 
								{
									rowSetData = new DataRow();

									String[] colInfo = line.split(",");
									for (int i : selectedColumnIndex) 
									{

										rowSetData.put(i,colInfo[i]);

									}
									if(!rowSetData.isEmpty())
									csvFileData.add(rowSetData);
									

								}
							}
						}

					}

				} 
				else if (queryParameters.getWhereCondition() ==true) 
				{
					
					List<Criteria> restrictions = queryParameters.getRestrictions();
					List<Integer> indexOfConditionColumn = new ArrayList<>();
					String operator=queryParameters.getRestrictions().get(0).getOperator();
					value=Integer.parseInt(queryParameters.getRestrictions().get(0).getValue());
					if (queryParameters.getLogicalOperator() == null) {
                          
                          
						for (String headerCol : headerColumn) {
							for (Criteria conditionColumns : restrictions) {
								if (headerCol.equals(conditionColumns.getColumnName())) {
									indexOfConditionColumn.add(headerColumn.indexOf(headerCol));
								}
							}
						}
                         switch (operator) {
						case ">":
							this.greaterThanOperation(selectedColumnIndex,indexOfConditionColumn,restrictions);
							
							break;
						case "<":
							this.lessThanOperation(selectedColumnIndex,indexOfConditionColumn,restrictions);
							break;
						case "=":
							this.equalOperation(selectedColumnIndex,indexOfConditionColumn,restrictions);
							break;
						default:
							break;
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		sortData(csvFileData);
		return csvFileData;
	}
	private List<DataRow> lessThanEqual(List<Integer> selectedColumnIndex, List<Integer> indexOfConditionColumn,
			List<Criteria> restrictions)throws Exception {
		int resultIndex = 0;
		
		for (int headerIndexs : headerIndex) {
			for (int columnIndex : selectedColumnIndex) {
				if (columnIndex == headerIndexs) {
					String line;
					while ((line = bufferedReader.readLine()) != null) {
						rowSetData = new DataRow();

						String[] colInfo = line.split(",");
						for (int i : selectedColumnIndex) {
							if(Integer.parseInt(colInfo[indexOfConditionColumn.get(0)])<=value)
							{
                         
							rowSetData.put(i,colInfo[i]);
							}

						}
						if(!rowSetData.isEmpty())
						csvFileData.add(rowSetData);
						

					}
				}
			}

		}
		sortData(csvFileData);
		return csvFileData;
		
	}
	private List<DataRow> equalOperation(List<Integer> selectedColumnIndex, List<Integer> indexOfConditionColumn,
			List<Criteria> restrictions) throws Exception {
		int resultIndex = 0;
		
		String value=queryParameters.getRestrictions().get(0).getValue();
		
		
		for (int headerIndexs : headerIndex) {
			for (int columnIndex : selectedColumnIndex) {
				if (columnIndex == headerIndexs) {
					String line;
					while ((line = bufferedReader.readLine()) != null) {
						rowSetData = new DataRow();

						String[] colInfo = line.split(",");
						
						for(int i:selectedColumnIndex)
						{
							
							if(colInfo[indexOfConditionColumn.get(0)].equals(value))
							{
								rowSetData.put(i,colInfo[i]);
							}
						}
						if(!rowSetData.isEmpty())
						csvFileData.add(rowSetData);
						
					}
				}
			}

		}
		sortData(csvFileData);
		return csvFileData;
	}
	private List<DataRow> lessThanOperation(List<Integer> selectedColumnIndex, List<Integer> indexOfConditionColumn,
			List<Criteria> restrictions) throws Exception {
		int resultIndex = 0;
		
		for (int headerIndexs : headerIndex) {
			for (int columnIndex : selectedColumnIndex) {
				if (columnIndex == headerIndexs) {
					String line;
					while ((line = bufferedReader.readLine()) != null) {
						rowSetData = new DataRow();

						String[] colInfo = line.split(",");
						for (int i : selectedColumnIndex) {
							if(Integer.parseInt(colInfo[indexOfConditionColumn.get(0)])<value)
							{
                         
							rowSetData.put(i,colInfo[i]);
							}

						}
						if(!rowSetData.isEmpty())
						csvFileData.add(rowSetData);
						

					}
				}
			}

		}
		sortData(csvFileData);
		return csvFileData;
		
	}
	int indexOfOrderbyColumn;
	private boolean sortData(List<DataRow> data)
	{
		
		String orderByColumn;
		orderByColumn=queryParameters.getOrderBy();
		
		if(queryParameters.getOrderBy()!=null)
		{
			for (String headerCol : headerColumn) {
				
					if(headerCol.equals(orderByColumn))
					{
						indexOfOrderbyColumn=headerColumn.indexOf(headerCol);
						
					}
				
			}
			
			DataSorting dataSort=new DataSorting();
			dataSort.setSortingIndex(indexOfOrderbyColumn);
			Collections.sort(data,dataSort);
			return true;
		}
		else
		{
			return false;
		}
		
	}
	private List<DataRow> greaterThanOperation(List<Integer> selectedColumnIndex, List<Integer> indexOfConditionColumn,
			List<Criteria> restrictions) throws Exception{
		
		int resultIndex = 0;
		
		String orderByColumn;
		
		String line;
		if(queryParameters.getFields().get(0).trim().equals("*"))
		{
			while((line=bufferedReader.readLine())!=null)
			{
				
				rowSetData = new DataRow();
				
				String[] colInfo = line.split(",");
				for (int i : selectedColumnIndex) {
					
					
					if(Integer.parseInt(colInfo[indexOfConditionColumn.get(0)])>value)
					{
						
					
					rowSetData.put(i,colInfo[i]);
					
					
					}
			}
				
				if(!rowSetData.isEmpty())
				csvFileData.add(rowSetData);
				
				
		}
			
			
		}
		else
		{
		for (int headerIndexs : headerIndex) {
			for (int columnIndex : selectedColumnIndex) {
				
				if (columnIndex == headerIndexs) {
					
					while ((line = bufferedReader.readLine()) != null) {
						rowSetData = new DataRow();
						
						String[] colInfo = line.split(",");
						for (int i : selectedColumnIndex) {
							if(Integer.parseInt(colInfo[indexOfConditionColumn.get(0)])>value)
							{
                         
							rowSetData.put(i,colInfo[i]);
							
							}
							
							
						}
						if(!rowSetData.isEmpty())
						csvFileData.add(rowSetData);
						

					}
					
				}
			}

		}
		}
	
		sortData(csvFileData);
		return csvFileData;
	}
}
