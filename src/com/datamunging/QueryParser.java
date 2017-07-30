package com.datamunging;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryParser {

	Query queryParameter;
	String mainQuery, conditionSection, selectSection, whereColumn, orderBy, groupBy, filePath;
	boolean allColumns;

	public QueryParser() {
		
		queryParameter = new Query();
	}

	private boolean isValidQuery(String queryString) {
		if (queryString.contains("select") && queryString.contains("from") || (queryString.contains("where")
				|| queryString.contains("order by") || queryString.contains("group by"))) {
			return true;
		} else {
			return false;
		}
	}

	public Query executeQuery(String query) {
		if (isValidQuery(query)) {

			queryParameter = this.parseQuery(query);
		} else {
			
		}

		return queryParameter;
	}

	private Query parseQuery(String queryString) {

		String splitQuery[];
		if (queryString.contains("where")) {
			
			splitQuery = queryString.split("where");
			
			queryString=splitQuery[0];
			queryParameter = this.whereOrderClause(splitQuery[1].trim());

		}
		
		
		queryParameter = this.selectwithOrderByClause(queryString.trim());
	
		return queryParameter;
}

	private Query selectwithOrderByClause(String selectOrderParameter) {

		String splitQuery[];
		String orderbyString = null;
		if (selectOrderParameter.contains("order by")) {
			splitQuery = selectOrderParameter.split("order by");
			orderbyString = this.orderbyClause(splitQuery[1]);
			queryParameter.setOrderBy(orderbyString);
			return this.selectGroupClause(splitQuery[0]);

		}
		else{
			String selectgroupParam = selectOrderParameter;

			queryParameter = this.selectGroupClause(selectgroupParam);
		}
		
		return queryParameter;
	}

	private Query selectGroupClause(String selectGroupParam) {
		// TODO Auto-generated method stub

		String splitQuery[];
		String groupbyString = null;
		if (selectGroupParam.contains("group")) {
			splitQuery = selectGroupParam.split("group by");
			groupbyString = this.groupbyClause(splitQuery[1]);
			queryParameter.setGroupBy(groupbyString);
			return this.selectToFromClause(splitQuery[0]);

		} 
		else
		{
			String selectParam = selectGroupParam;

			queryParameter = this.selectToFromClause(selectParam);
		}
		
		return queryParameter;
	}

	private Query selectToFromClause(String selectFromParameter) {

		List<String> fieldlist = new ArrayList<>();
		String pattern1 = "select(.*)from(.*)";
		Pattern pattern = Pattern.compile(pattern1);
		List<AggregateFunction> aggregateFunctionlist=new ArrayList<>();

		String filename = null;
		Matcher matcher4 = pattern.matcher(selectFromParameter);
		List<String> list=new ArrayList<>();
		if (matcher4.find()) {
			if (matcher4.group(1).contains("*")) {
				
				queryParameter.setAllColumns(true);
				fieldlist.add(matcher4.group(1));
				
			} else {
				String[] fielditem = matcher4.group(1).split(",");	
				for (String field : fielditem) {
					
					if (field.contains("sum") || field.contains("avg") || field.contains("min")
							|| field.contains("max") || field.contains("count")) {
						
						AggregateFunction aggregateFunction=new AggregateFunction();
						String[] splitfield1 = field.split("\\(");
						aggregateFunction.setFunctionName(splitfield1[0]);
						
						String[] splitfield2 = splitfield1[1].split("\\)");
						aggregateFunction.setFunctionColumn(splitfield2[0]);
						
						aggregateFunctionlist.add(aggregateFunction);
					}
					else
					{
					fieldlist.add(field.trim());
					}
					}
				}
			}

			filename = matcher4.group(2);
			
			queryParameter.setFields(fieldlist);
			queryParameter.setFilePath(filename.trim());
			queryParameter.setAggregateFunctions(aggregateFunctionlist);
			return queryParameter;

	}

	private Query whereOrderClause(String whereOrderParam) {
		// TODO Auto-generated method stub
	
		String splitQuery[];
		String orderbyString = null;
		if (whereOrderParam.contains("order by")) {
			splitQuery = whereOrderParam.split("order by");
			orderbyString = this.orderbyClause(splitQuery[1]);
			queryParameter.setOrderBy(orderbyString);
			
			return this.whereGroupClause(splitQuery[0]);

		} else {
			
			String wheregroupParam = whereOrderParam;
			
			queryParameter = this.whereGroupClause(wheregroupParam);

		}
		return queryParameter;
	}

	private Query whereGroupClause(String whereGroupParam) {
		// TODO Auto-generated method stub

		String splitQuery[];
		String groupbyString = null;
		if (whereGroupParam.contains("group by")) {
			splitQuery = whereGroupParam.split("group by");
			groupbyString = this.groupbyClause(splitQuery[1]);
			queryParameter.setGroupBy(groupbyString);
			return this.onlyWhereClause(splitQuery[0]);

		} else {
		
			String whereParameter = whereGroupParam;
		
			queryParameter = this.onlyWhereClause(whereParameter);
			queryParameter.setWhereCondition(true);
		}
		
		return queryParameter;
	}

	private Query onlyWhereClause(String whereParam) {
		queryParameter.setWhereCondition(true);
		String pattern = "(.*)";
		// to store column operator and value
		
		
		// add criteria object List<criteria>
		List<Criteria> listCriteria = new ArrayList<>();
		String[] whereArrayConditions = null;
		// to store list of logical operator
		List<String> logicalOperator = new ArrayList<>();
		// splitting condition through operator
		String[] splitCondition = null;
		String[] patternrelation;
		Pattern wherePattern = Pattern.compile(pattern);
		Matcher whereMatcher = wherePattern.matcher(whereParam);

		if (whereMatcher.find()) {
			
			
			whereArrayConditions = whereParam.split(" ");

			for (String condition : whereArrayConditions) {
				
				if ((condition.trim().equals("AND")) ||(condition.trim().equals("and"))|| (condition.trim().equals("or"))||((condition.trim().equals("OR"))) || (condition.trim().equals("!")))
				{
					logicalOperator.add(condition.trim());

				} else {
					Criteria criteria = new Criteria();

					patternrelation = condition.split("([<|>|!|=])+");
					
					
					criteria.setColumnName(patternrelation[0]);
					criteria.setValue(patternrelation[1]);
					
					int startIndex = condition.indexOf(patternrelation[0]) + patternrelation[0].length();
					int endIndex = condition.indexOf(patternrelation[1]);
					String operator = condition.substring(startIndex, endIndex).trim();	
					criteria.setOperator(operator);

					// add criteria object to list
					listCriteria.add(criteria);
				}
			}
			queryParameter.setRestrictions(listCriteria);
		}
		
		queryParameter.setLogicalOperator(logicalOperator);
		
		return queryParameter;

	}

	private String groupbyClause(String groupByParam) {
		String pattern1 = "(.*\\S)";
		Pattern pattern = Pattern.compile(pattern1);
		Matcher matcher = pattern.matcher(groupByParam);

		String groupByString = null;

		if (matcher.find()) {
			groupByString = matcher.group(1);

		}
		return groupByString;

	}

	private String havingClause(String havingParam) {
		// TODO Auto-generated method stub

		String pattern1 = "(.*\\S)";
		Pattern pattern = Pattern.compile(pattern1);
		Matcher matcher = pattern.matcher(havingParam);

		String havingString = null;

		if (matcher.find()) {
			havingString = matcher.group(1);

		}
		
		return havingString;
	}

	private String orderbyClause(String orderbyParam) {

		String pattern = "(.*\\S)";
		Pattern pattern1 = Pattern.compile(pattern);
		Matcher matcher = pattern1.matcher(orderbyParam);

		String orderbyString = null;

		if (matcher.find()) {
			orderbyString = matcher.group(1);

		}
		return orderbyString;

	}
/*
private Query querySelectToFrom(String parameter) {

	String pattern = "select(.*)from(.*)";
	Pattern pattern1 = Pattern.compile(pattern);
	List<String> fieldlist = new ArrayList<>();
	String filename = null;
	Matcher matcher = pattern1.matcher(parameter);

	if (matcher.find()) {
		if (matcher.group(1).contains("*")) {
			fieldlist.add(matcher.group(1));
			queryParameter.setAllColumns(true);
		} else {
			String[] fielditem = matcher.group(1).split(",");
			for (String field : fielditem) {
				fieldlist.add(field.trim());

			}
		}
		filename = matcher.group(2);
		queryParameter.setFields(fieldlist);
		queryParameter.setFilePath(filename.trim());
	}
	return queryParameter;
}

private Query whereClause(String beforeWhere,String whereParam) {
	
	queryParameter=this.querySelectToFrom(beforeWhere);
	String pattern = "(.*)";
	Pattern pattern1 = Pattern.compile(pattern);
	Matcher matcher = pattern1.matcher(whereParam);
	List<Criteria> conditions = new ArrayList<>();
	List<String> logicalOperator=new ArrayList<>();
	String whereString = null;
	
	logicalOperator=null;
	if (matcher.find()) {
		whereString = matcher.group(1).trim();
		String[] whereArray = whereString.split(" ");
		for (String condition : whereArray) {
			Criteria criteria = new Criteria();
			if ((condition.equals("group")) || (condition.equals("order"))) {
				break;
			}
			if(condition.equals("and") || condition.equals("or"))
			{
				logicalOperator.add(condition);
			}
			String[] colName = condition.split("([<|>|!|=])+");
			int startIndex = condition.indexOf(colName[0]) + colName[0].length();
			int endIndex = condition.indexOf(colName[1]);
			String operator = condition.substring(startIndex, endIndex).trim();
			criteria.setColumnName(colName[0].trim());
			criteria.setOperator(operator);
			criteria.setValue(colName[1].trim());
			conditions.add(criteria);
			queryParameter.setWhereCondition(condition);
		}
		
		queryParameter.setRestrictions(conditions);
		queryParameter.setLogicalOperator(logicalOperator);
		
	}

	return queryParameter;
}

private Query groupbyClause(String groupByParam) {
	String pattern = "(.*)";
	Pattern pattern1 = Pattern.compile(pattern);
	Matcher matcher = pattern1.matcher(groupByParam);

	String[] groupByString = null;

	if (matcher.find()) {
		groupByString = matcher.group().split(" ");
		for (String group : groupByString) {
			if (group.equals("order")) {
				break;
			}
			queryParameter.setGroupBy(group);
		}

	}

	return queryParameter;

}

private void havingClause(String havingParam) {

	String pattern = "(.*)";
	Pattern pattern1 = Pattern.compile(pattern);
	Matcher matcher = pattern1.matcher(havingParam);

	String havingString = null;

	if (matcher.find()) {
		havingString = matcher.group(1);

	}

}

private Query orderby(String orderbyParam) {

	String pattern = "(.*)";
	Pattern pattern1 = Pattern.compile(pattern);
	Matcher matcher = pattern1.matcher(orderbyParam);

	String orderbyString = null;

	if (matcher.find()) {
		orderbyString = matcher.group();

	}
	
	queryParameter.setOrderBy(orderbyString.trim());
	return queryParameter;
}*/
}
