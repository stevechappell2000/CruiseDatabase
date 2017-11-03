package com.cruise.plugins.CruiseDatabase.metadata;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map.Entry;

import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.services.utils.CollectionBean;
import com.corecruise.cruise.services.utils.Services;

public class MDLoader {
    public static boolean loadMetaData(DatabaseMetaData mData, SessionObject so, Services service, DBMetaData dmd) throws Exception {
    	boolean ret = false;
    	ResultSet rs = null;
    	TableMetaData t = null;
		try {
			rs = mData.getTables(dmd.getCatalog(), dmd.getSchema(), "%", dmd.getTypes());
			while(rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				t = new TableMetaData(tableName);
				t.setTableName(rs.getString("TABLE_NAME"));
				t.setTableType(rs.getString("TABLE_TYPE"));
				loadColumnData(mData,so,service, dmd,t);
				loadPrimaryKeys(mData,so,service, dmd,t);
				loadForeignKeys(mData,so,service, dmd,t);
				loadKeys(mData,so,service, dmd,t);
				dmd.getTables().put(tableName, t);
				ret = true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}finally {
			if(null != rs) {
				rs.close();
			}
		}
		return ret;
    }
    private static void loadColumnData(DatabaseMetaData mData, SessionObject so, Services service, DBMetaData dmd, TableMetaData tmd) {
		int i = 0;
		ResultSet rs = null;
		try {
			rs = mData.getColumns(dmd.getCatalog(), dmd.getSchema(), tmd.getTableName(), "%");
			HashMap<String, ColumnMetaData> tcm = tmd.getColumns();
			while(rs.next()) {
				ColumnMetaData cb = new ColumnMetaData();
				cb.getColumnMetaData().put("TYPE_NAME",rs.getString("TYPE_NAME"));
				cb.getColumnMetaData().put("COLUMN_SIZE",rs.getString("COLUMN_SIZE"));
				cb.getColumnMetaData().put("DECIMAL_DIGITS",rs.getString("DECIMAL_DIGITS"));
				cb.getColumnMetaData().put("IS_AUTOINCREMENT",rs.getString("IS_AUTOINCREMENT"));
				cb.getColumnMetaData().put("COLUMN_DEF",rs.getString("COLUMN_DEF"));
				cb.getColumnMetaData().put("NULLABLE",rs.getString("NULLABLE"));
				cb.getColumnMetaData().put("COLUMN_NAME",rs.getString("COLUMN_NAME"));
				cb.getColumnMetaData().put("COLUMN_NAME",rs.getString("COLUMN_NAME"));
				tmd.getColumns().put(rs.getString("COLUMN_NAME"), cb);

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}finally {
			if(null != rs) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
    }
	private static void loadPrimaryKeys(DatabaseMetaData mData, SessionObject so, Services service, DBMetaData dmd, TableMetaData tmd) {
		ResultSet res = null;
		String CTable = tmd.getTableName();
		HashMap<String, ColumnMetaData> tcm = tmd.getColumns();
		if(!tmd.getTableType().equalsIgnoreCase("table")) {
			//System.out.println("NOT A TABLE:"+tmd.getTableType());
			return;
		}
		try{
			//res = mData.getImportedKeys(dmd.getCatalog(), dmd.getSchema(), CTable);
			res = mData.getPrimaryKeys(dmd.getCatalog(),dmd.getSchema(),CTable);

			while(res.next()){
				String keyColumn = res.getString("COLUMN_NAME");
				for(Entry<String, ColumnMetaData> f: tcm.entrySet()){
					if(f.getKey().equalsIgnoreCase(keyColumn)){
						f.getValue().getColumnMetaData().put("KEY", "true");
						f.getValue().getColumnMetaData().put("PrimaryKey","true");
						f.getValue().getColumnMetaData().put("PKNAME", res.getString("PK_NAME"));
						f.getValue().getColumnMetaData().put("KEYSEQ", res.getString("KEY_SEQ"));
						break;
					}
				}
			}
	    }catch(Exception e){
	    	e.printStackTrace();
		}finally{
			if(null != res)
				try {
					res.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	private static void loadForeignKeys(DatabaseMetaData mData, SessionObject so, Services service, DBMetaData dmd, TableMetaData tmd) {
		//
		ResultSet res = null;
		String CTable = tmd.getTableName();
		HashMap<String, ColumnMetaData> tcm = tmd.getColumns();
		if(!tmd.getTableType().equalsIgnoreCase("table")) {
			//System.out.println("NOT A TABLE:"+tmd.getTableType());
			return;
		}
		try{
			res = mData.getImportedKeys(dmd.getCatalog(), dmd.getSchema(), CTable);
			while(res.next()){
				String keyColumn = res.getString("COLUMN_NAME");
				for(Entry<String, ColumnMetaData> f: tcm.entrySet()){
					if(f.getKey().equalsIgnoreCase(keyColumn)){
						f.getValue().getColumnMetaData().put("KEY", "true");//setKey(true);
						f.getValue().getColumnMetaData().put("DELETE_RULE", res.getString("DELETE_RULE"));
						f.getValue().getColumnMetaData().put("UPDATE_RULE",res.getString("UPDATE_RULE"));
						f.getValue().getColumnMetaData().put("FKEYTABLE_NAME",res.getString("FKTABLE_NAME"));
						f.getValue().getColumnMetaData().put("FKEYCOLUMN_NAME",res.getString("FKCOLUMN_NAME"));
						f.getValue().getColumnMetaData().put("KEY_SEQ",res.getString("KEY_SEQ"));
                 
					}
				}
			}

	    }catch(Exception e){
	    	e.printStackTrace();
		}finally{
			if(null != res)
				try {
					res.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	private static void loadKeys(DatabaseMetaData mData, SessionObject so, Services service, DBMetaData dmd, TableMetaData tmd){
		ResultSet res = null;
		String CTable = tmd.getTableName();
		HashMap<String, ColumnMetaData> tcm = tmd.getColumns();
		if(!tmd.getTableType().equalsIgnoreCase("table")) {
			//System.out.println("NOT A TABLE:"+tmd.getTableType());
			return;
		}
		try{
			res = mData.getIndexInfo(dmd.getCatalog(), dmd.getSchema(), CTable, false, true);
			while(res.next()){
				String keyColumn = res.getString("COLUMN_NAME");
				for(Entry<String, ColumnMetaData> f: tcm.entrySet()){
					if(f.getKey().equalsIgnoreCase(keyColumn)){
						f.getValue().getColumnMetaData().put("KEY", "true");//setIkey(true);
						f.getValue().getColumnMetaData().put("KEYTABLENAME",res.getString("TABLE_NAME"));
						f.getValue().getColumnMetaData().put("INDEX_NAME",res.getString("INDEX_NAME"));
						f.getValue().getColumnMetaData().put("NONE_UNIQUE",res.getString("NON_UNIQUE"));
					}
				}
			}

		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != res)
				try {
					res.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

}
