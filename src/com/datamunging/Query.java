package com.datamunging;

import java.util.List;



public class Query {

	private List<String> fields;
	private String filePath;
	private boolean whereCondition;
	private List<Criteria> restrictions;
	private boolean allColumns;
	private String groupBy;
	private String orderBy;
	private List<String> logicalOperator;
	private List<AggregateFunction> aggregateFunctions;
	
	public List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunctions;
	}
	public void setAggregateFunctions(List<AggregateFunction> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}
	public List<String> getFields() {
		return fields;
	}
	public void setFields(List<String> fields) {
		this.fields = fields;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public boolean getWhereCondition() {
		return whereCondition;
	}
	public void setWhereCondition(boolean whereCondition) {
		this.whereCondition = whereCondition;
	}
	public List<Criteria> getRestrictions() {
		return restrictions;
	}
	public void setRestrictions(List<Criteria> restrictions) {
		this.restrictions = restrictions;
	}
	public boolean isAllColumns() {
		return allColumns;
	}
	public void setAllColumns(boolean allColumns) {
		this.allColumns = allColumns;
	}
	public String getGroupBy() {
		return groupBy;
	}
	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}
	public String getOrderBy() {
		return orderBy;
	}
	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
		
	}
	public List<String> getLogicalOperator() {
		return logicalOperator;
	}
	public void setLogicalOperator(List<String> logicalOperator) {
		this.logicalOperator = logicalOperator;
	}
	
}
