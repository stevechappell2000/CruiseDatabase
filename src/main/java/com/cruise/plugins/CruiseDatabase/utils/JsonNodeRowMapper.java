package com.cruise.plugins.CruiseDatabase.utils;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;

import com.corecruise.cruise.SessionObject;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class JsonNodeRowMapper  {

    public static boolean mapRows(SessionObject so, ResultSet rs) throws SQLException {
    	boolean ret = false;
    	ResultSetMetaData rsmd = rs.getMetaData();
    	int columnCount = rsmd.getColumnCount();
    	ArrayNode retMap = mapRows(so,rs,rsmd,columnCount);
    	if(null != retMap) {
    		ret = true;
    		so.appendToResponse(retMap);
    	}
    	return ret;
    }
    private static ArrayNode mapRows(SessionObject so, ResultSet rs, ResultSetMetaData rsmd, int columnCount ) throws SQLException {
    	ObjectNode objectNode = null;//so.getCruiseMapper().createObjectNode();
    	ArrayNode arrayNode = so.getCruiseMapper().createArrayNode();

    	while(rs.next()) {
    		objectNode = so.getCruiseMapper().createObjectNode();
    		for (int index = 1; index <= columnCount; index++) {
    			String column = rsmd.getColumnName(index);
    			Object value = rs.getObject(column);
    			if (value == null) {
    				objectNode.putNull(column);
    			} else if (value instanceof Integer) {
    				objectNode.put(column, (Integer) value);
    			} else if (value instanceof String) {
    				objectNode.put(column, (String) value);                
    			} else if (value instanceof Boolean) {
    				objectNode.put(column, (Boolean) value);           
    			} else if (value instanceof Date) {
    				objectNode.put(column, ((Date) value).getTime());                
    			} else if (value instanceof Long) {
    				objectNode.put(column, (Long) value);                
    			} else if (value instanceof Double) {
    				objectNode.put(column, (Double) value);                
    			} else if (value instanceof Float) {
    				objectNode.put(column, (Float) value);                
    			} else if (value instanceof BigDecimal) {
    				objectNode.put(column, (BigDecimal) value);
    			} else if (value instanceof Byte) {
    				objectNode.put(column, (Byte) value);
    			} else if (value instanceof byte[]) {
    				objectNode.put(column, (byte[]) value);                
    			} else {
    				throw new IllegalArgumentException("Unmappable object type: " + value.getClass());
    			}
    		}
    		arrayNode.add(objectNode);
    	}
		return arrayNode;
    	
    }

}
