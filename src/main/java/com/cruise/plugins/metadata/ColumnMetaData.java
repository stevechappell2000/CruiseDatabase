package com.cruise.plugins.metadata;

import java.util.HashMap;
import com.fasterxml.jackson.annotation.JsonIgnore;


public class ColumnMetaData {
	HashMap<String,String> columnMetaData = new HashMap<String,String>();
	
	@JsonIgnore
	public HashMap<String,String> getColumnMetaData() {
		return columnMetaData;
	}
	@JsonIgnore
	public HashMap<String,String> getColumnInfo(){
		return columnMetaData;
	}
	@JsonIgnore
	public void setColumnMetaData(HashMap<String,String> columnMetaData) {
		this.columnMetaData = columnMetaData;
	}
	
	/*String name = null;
	String type = null;
	String size = null;
	String defaultValue = null;
	String Des = null;
	String autoInc = null;
	String colDefault = null;
	String allowNull = null;
	String pkeyName = null;
	Short pkeySequence = null;
	Short fkeySequence = null;
	Short cascadeDelete = null;
	Short cascadeUpdate = null;
	String fKeyTableName = null;
	String fKeyName = null;
	boolean key = false;
	boolean pkey = false;
	boolean ikey = false;
	String iKeyTableName = null;
	String iKeyName = null;
	boolean iKeyUnique = false;
    int order = 0;
	boolean primaryKey = false;
	boolean foriegnKey = false;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getSize() {
		return size;
	}
	public void setSize(String size) {
		this.size = size;
	}
	public String getDefaultValue() {
		return defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	public boolean isPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(boolean primaryKey) {
		this.primaryKey = primaryKey;
	}
	public boolean isForiegnKey() {
		return foriegnKey;
	}
	public void setForiegnKey(boolean foriegnKey) {
		this.foriegnKey = foriegnKey;
	}
	public String getDes() {
		return Des;
	}
	public void setDes(String des) {
		Des = des;
	}

	public String getAutoInc() {
		return autoInc;
	}
	public void setAutoInc(String autoInc) {
		this.autoInc = autoInc;
	}
	public String getColDefault() {
		return colDefault;
	}
	public void setColDefault(String colDefault) {
		this.colDefault = colDefault;
	}
	public String getAllowNull() {
		return allowNull;
	}
	public void setAllowNull(String allowNull) {
		this.allowNull = allowNull;
	}
	public int getOrder() {
		return order;
	}
	public void setOrder(int order) {
		this.order = order;
	}
	public String getPkeyName() {
		return pkeyName;
	}
	public void setPkeyName(String pkeyName) {
		this.pkeyName = pkeyName;
	}
	public Short getPkeySequence() {
		return pkeySequence;
	}
	public void setPkeySequence(Short pkeySequence) {
		this.pkeySequence = pkeySequence;
	}
	public Short getCascadeDelete() {
		return cascadeDelete;
	}
	public void setCascadeDelete(Short cascadeDelete) {
		this.cascadeDelete = cascadeDelete;
	}
	public Short getCascadeUpdate() {
		return cascadeUpdate;
	}
	public void setCascadeUpdate(Short cascadeUpdate) {
		this.cascadeUpdate = cascadeUpdate;
	}
	public String getfKeyTableName() {
		return fKeyTableName;
	}
	public void setfKeyTableName(String fKeyTableName) {
		this.fKeyTableName = fKeyTableName;
	}
	public String getfKeyName() {
		return fKeyName;
	}
	public void setfKeyName(String fKeyName) {
		this.fKeyName = fKeyName;
	}
	public boolean isKey() {
		return key;
	}
	public void setKey(boolean key) {
		this.key = key;
	}
	public boolean isPkey() {
		return pkey;
	}
	public void setPkey(boolean pkey) {
		this.pkey = pkey;
	}
	public Short getFkeySequence() {
		return fkeySequence;
	}
	public void setFkeySequence(Short fkeySequence) {
		this.fkeySequence = fkeySequence;
	}
	public boolean isIkey() {
		return ikey;
	}
	public void setIkey(boolean ikey) {
		this.ikey = ikey;
	}
	public String getiKeyTableName() {
		return iKeyTableName;
	}
	public void setiKeyTableName(String iKeyTableName) {
		this.iKeyTableName = iKeyTableName;
	}
	public String getiKeyName() {
		return iKeyName;
	}
	public void setiKeyName(String iKeyName) {
		this.iKeyName = iKeyName;
	}
	public boolean isiKeyUnique() {
		return iKeyUnique;
	}
	public void setiKeyUnique(boolean iKeyUnique) {
		this.iKeyUnique = iKeyUnique;
	}
*/
	
	
}
