package com.cruise.plugins.CruiseDatabase.utils;

public class QueryFieldValue{
	String FieldName;
	String FieldValue;
	String FieldType;
	boolean excludeFromWhere = false;
	public QueryFieldValue(String FN, String FV, String type, boolean Xclude){
		FieldName = FN;
		FieldValue = FV;
		FieldType = type;
		excludeFromWhere = Xclude;
	}
	public String getFieldName() {
		return FieldName;
	}
	public void setFieldName(String fieldName) {
		FieldName = fieldName;
	}
	public String getFieldValue() {
		return FieldValue;
	}
	public void setFieldValue(String fieldValue) {
		FieldValue = fieldValue;
	}
	public boolean isExcludeFromWhere() {
		return excludeFromWhere;
	}
	public String getFieldType() {
		return FieldType;
	}
	public void setFieldType(String fieldType) {
		FieldType = fieldType;
	}
}
