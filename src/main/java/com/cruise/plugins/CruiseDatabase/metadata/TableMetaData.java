package com.cruise.plugins.CruiseDatabase.metadata;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
//@JsonFormat(shape=JsonFormat.Shape.ARRAY)
public class TableMetaData {
	//@JsonIgnore
	String tableName = null;
	//@JsonIgnore
	String tableType = null;
	@JsonIgnore
	HashMap<String, ColumnMetaData> columns = new HashMap<String, ColumnMetaData>();
	public TableMetaData(String tableName2) throws SQLException {
		tableName = tableName2;

	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTableType() {
		return tableType;
	}
	public void setTableType(String tableType) {
		this.tableType = tableType;
	}
	@JsonIgnore
	public ArrayList<String> getFieldNames(){
		ArrayList<String> out = new ArrayList<String>();
		for(Entry<String, ColumnMetaData> f: columns.entrySet()){
			for(Entry<String,String> s: f.getValue().columnMetaData.entrySet()){
				try{
					if(null != s.getValue()) {
						if(s.getKey().equalsIgnoreCase("COLUMN_NAME")) {
							out.add(s.getValue());
						}
					}
				}catch(Exception e) {
					System.out.println(s);
				}
			}
		}
		return out;
	}
	@JsonCreator
	public ArrayList<Properties> getAllColumns(){
		
		ArrayList<Properties> out = new ArrayList<Properties>();
		for(Entry<String, ColumnMetaData> f: columns.entrySet()){
			//for(Entry<String,String> s: f.getValue().columnMetaData.entrySet()){
			Properties p = new Properties();
			for(Entry<String,String> s: f.getValue().columnMetaData.entrySet()){
				try{
					if(null != s.getValue()) {
						p.put(s.getKey(), s.getValue());
					}else {
						p.put(s.getKey(), "");
					}
				}catch(Exception e) {
					System.out.println(s);
				}
			}
            out.add(p);
		}
		return out;
	}
	@JsonIgnore
	public ColumnMetaData getTableColumns(String tableName) {
		ColumnMetaData ret = null;
		if(columns.containsKey(tableName)) {
			ret =  columns.get(tableName);
		}
		return ret;
	}
	@JsonIgnore
	public HashMap<String, ColumnMetaData> getColumns() {
		return columns;
	}
	@JsonIgnore
	public void setColumns(HashMap<String, ColumnMetaData> columns) {
		this.columns = columns;
	}
	
}
