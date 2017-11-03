package com.cruise.plugins.CruiseDatabase.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import com.fasterxml.jackson.annotation.JsonIgnore;

//@JsonPropertyOrder({ "id", "name" })
public class DBMetaData {
	String databaseName = null;
	String vendor = null;
	String schema = null;
	String catalog = null;
	String[] types = {"TABLE", "VIEW"};
	Properties connectionProperties = new Properties();
	HashMap<String,TableMetaData> tables = new HashMap<String,TableMetaData>();
	public DBMetaData(Properties p) {
		connectionProperties = p;
		if(null != p) {
			if(p.containsKey("schema")) {
				setSchema(p.getProperty("schema"));
			}
			if(p.containsKey("catalog")) {
				setSchema(p.getProperty("catalog"));
			}
		}
	}
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public String getVendor() {
		return vendor;
	}
	public void setVendor(String vendor) {
		this.vendor = vendor;
	}
	public Properties getConnectionProperties() {
		return connectionProperties;
	}
	public void setConnectionProperties(Properties connectionProperties) {
		this.connectionProperties = connectionProperties;
	}
	@JsonIgnore
	public HashMap<String, TableMetaData> getTables() {
		return tables;
	}
	@JsonIgnore
	public void setTables(HashMap<String, TableMetaData> tables) {
		this.tables = tables;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getCatalog() {
		return catalog;
	}
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	public String[] getTypes() {
		return types;
	}
	public void setTypes(String[] types) {
		this.types = types;
	}
    public ArrayList<Properties> getTableList(){
		ArrayList<Properties> out = new ArrayList<Properties>();
		for(Entry<String, TableMetaData> f: tables.entrySet()){
			Properties p = new Properties();
			p.put("Columns", ""+f.getValue().getAllColumns());
			p.put("Type",f.getValue().getTableType());
			p.put("TableName",f.getValue().getTableName());
            out.add(p);
		}
		return out;
    } 
    
}
