package com.cruise.plugins.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;
import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.utils.Services;
import com.cruise.plugins.metadata.ColumnMetaData;
import com.cruise.plugins.metadata.DBMetaData;
import com.cruise.plugins.metadata.TableMetaData;

public class QueryBuilder {
	private static String dataFormat = "yyyy-mm-dd hh24:mi:ss.FF";
    private static String valueCleanup(String fieldName, String fieldValue, String Type) throws Exception {
    	String ret = null;
    	String getType = getTypeConName(Type,0,0);
    	try {
			ret = convertString(getType,fieldValue);
		} catch (Exception e) {
			throw e;
		}
    	
    	return ret;
    }
	public static String createUpdate(SessionObject so, Services service) throws SQLException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, Exception{
		try{
			DBMetaData dbmd = cruConnection.getPoolMetaData(so, service);
			if(null == dbmd) {
				return null;
			}
			String vendorName = dbmd.getVendor();
			String SchemaName = dbmd.getSchema();
			StringBuffer sbQ 	= new StringBuffer();
			StringBuffer fields = new StringBuffer();
			StringBuffer tables = new StringBuffer();
			StringBuffer where 	= new StringBuffer();
			String[] froms = null;
			HashMap<String,String> PKeys 	= new HashMap<String,String>();
			HashMap<String,String> Keys 	= new HashMap<String,String>();
			ArrayList<QueryFieldValue> qfv = new ArrayList<QueryFieldValue>();
			String xTable = "";
			String qFields = service.Parameter("columnList");
			String qFrom = service.Parameter("tableName");

			if(null != qFrom && qFrom.contains(",")) {
				froms = qFrom.split(",");
			}else {
				froms = new String[] {qFrom};
			}
			String timeStampFunctionName = "TIMESTAMP";
			int i = 0;
			for(i = 0;i<froms.length;i++){
				ArrayList<String> getM = null;
				TableMetaData metric = dbmd.getTables().get(froms[i]);
				
				boolean pk = false;
				HashMap<String, ColumnMetaData> TableFields = null;

				if(vendorName.contains("oracle")){
					timeStampFunctionName = "TO_TIMESTAMP";
				}else if(vendorName.contains("sql server")){
					timeStampFunctionName = "";
				}

				xTable = froms[i];
				if(null != SchemaName && SchemaName.trim().length()>0){
					tables.append(SchemaName+"."+froms[i]);
				}else{
					tables.append(froms[i]);
				}
				TableFields = metric.getColumns();//.get(froms[i]).getColumnMetaData();
				if(i > 0)
					tables.append(",");

				if(null != qFields ){
					getM = (ArrayList<String>) Arrays.asList(qFields.split(","));
				}else{
					getM = metric.getFieldNames();
				}
				for(int g=0;g<getM.size();g++){

					String strF = null;
					String type = null;
					String CoreType = null;
					strF = TableFields.get(getM.get(g)).getColumnInfo().get("COLUMN_NAME");
					type = TableFields.get(getM.get(g)).getColumnInfo().get("TYPE_NAME");
					CoreType = TableFields.get(getM.get(g)).getColumnInfo().get("TYPE_NAME");
					pk = false;
					if(TableFields.get(getM.get(g)).getColumnInfo().containsKey("PrimaryKey")) {
						if(TableFields.get(getM.get(g)).getColumnInfo().get("PrimaryKey").equalsIgnoreCase("true")) {
							pk = true;
						}
					}

					String fieldName = xTable+"."+strF;
					String fieldValue = null;
					boolean excludeFromWhere = false;//obj.excludeFromWhere(fieldName);
					if( null != CoreType  && (CoreType.equalsIgnoreCase("CLOB")|| CoreType.equalsIgnoreCase("BLOB"))) {
						excludeFromWhere = true;
					}
					if(Keys.containsKey(strF)){
						qfv.add(new QueryFieldValue((Keys.get(strF)+"."+strF), fieldName, type, excludeFromWhere));
					}else{
						try{
							fieldValue = service.Parameter(strF);
							if(null != fieldValue && type.equalsIgnoreCase("boolean")){
								if(fieldValue.equals("false")){
									fieldValue = "0";
								}else{
									fieldValue = "1";
								}
							}
							//boolean otherPKey = false;
							
							if(PKeys.containsKey(strF)){
								fieldValue = PKeys.get(strF)+"."+strF;
								qfv.add(new QueryFieldValue(fieldName,fieldValue, type, true));
							}else if(pk){//metric.getTableMetrics().isPrimaryKey(strF)){
								PKeys.put(strF,xTable);
								qfv.add(new QueryFieldValue(fieldName,fieldValue, type, true));

							}else {
								Keys.put(strF, xTable);
								qfv.add(new QueryFieldValue(fieldName, fieldValue, type, false));

							}
						}catch(Exception e){

						}
					}
				}
			}

			//setLastKeys(PKeys);
			if(qfv.size()>0){
				for(int f=0;f<qfv.size();f++){
					String fv = qfv.get(f).getFieldValue();
					String fn = qfv.get(f).getFieldName();
					String ty = qfv.get(f).getFieldType();
					String fvFinished = valueCleanup(fn,fv,ty);
					if(!qfv.get(f).isExcludeFromWhere()){
						if(null != fv && fv.length()>0){
							if(fields.toString().length()>0)
								fields.append(",\n");
							if(qfv.get(f).getFieldType().equalsIgnoreCase("timestamp") || qfv.get(f).getFieldType().equalsIgnoreCase("date")){
								fv = fv.replaceAll("\'", "").trim();
								if(fv.length()>0){
									if(fv.trim().length()<11 && fv.contains(".") == false){
										fv = fv.trim()+" 00:00:00";
									}
									try{
										java.sql.Timestamp tsd = java.sql.Timestamp.valueOf(fv);
										if(vendorName.contains("oracle")){
											fields.append(fn+"=("+timeStampFunctionName+"  ('"+tsd+"','"+dataFormat+"'))");
										}else if(vendorName.contains("sql server")){
											fields.append(fn+"='"+fv+"'");
										}else{
											fields.append(fn+"=("+timeStampFunctionName+"  ('"+tsd+"'))");
										}
									}catch(Exception e){
										if(fv.contains(".")){
											fields.append(fn+"="+fv+"");
										}else{
											fields.append(fn+"='"+fv+"'");
										}
									}
								}
							}else if((qfv.get(f).getFieldType().equalsIgnoreCase("clob") || qfv.get(f).getFieldType().equalsIgnoreCase("blob"))){
								if(vendorName.contains("oracle")){
									fields.append(fn+"= ?");
								}else{
									fields.append(fn+"="+fvFinished);
								}
							}else if(fv.length()> 3000 && vendorName.contains("oracle") && (qfv.get(f).getFieldType().equalsIgnoreCase("clob") || qfv.get(f).getFieldType().equalsIgnoreCase("blob"))){
								fields.append(fn+"="+splitClob(fv.substring(1,fv.length()-1),3000));
							}else{
								fields.append(fn+"="+fvFinished);
							}
						}else{
							//if(!partialUpdate){
								if(fields.toString().length()>0)
									fields.append(",");
								fields.append(fn+"=null");
							//}
						}
					}else if((null != fv)){
						if(where.toString().length()>0)
							where.append(" and ");
						where.append(fn+"="+fvFinished);
					}
				}
			}
			//update table
			sbQ.append("Update ");
			if(null != SchemaName && SchemaName.trim().length()>0){
				sbQ.append(" "+SchemaName+"."+xTable+ " set "+fields);
			}else{
				sbQ.append(" "+xTable+ " set "+fields);
			}
			
			if(where.length()>0){
				sbQ.append(" where "+where.toString());
			}else{
				throw new Exception("Error with Update. No qualifying 'where' clause detected.\n"+sbQ.toString());
			}
			return sbQ.toString();
			// throws SQLException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, Exception
		}catch(IllegalAccessException err){
			throw err;
		}catch(IllegalArgumentException err){
			throw err;
		}catch(InvocationTargetException err){
			throw err;
		}catch(Exception err){
			throw err;
		}

	}
	
	
	public static String createSelect(SessionObject so, Services service, boolean bAll) throws SQLException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, Exception{
		try{
			DBMetaData dbmd = cruConnection.getPoolMetaData(so, service);
			if(null == dbmd) {
				return null;
			}
			String vendorName = dbmd.getVendor();
			String SchemaName = dbmd.getSchema();
			StringBuffer sbQ 	= new StringBuffer();
			StringBuffer sbQCount 	= new StringBuffer();
			StringBuffer fields = new StringBuffer();
			StringBuffer tables = new StringBuffer();
			StringBuffer where 	= new StringBuffer();
			//StringBuffer OrderBy = new StringBuffer();
			String[] froms = null;
			HashMap<String, String> keys 		= new HashMap<String, String>();
			ArrayList<QueryFieldValue> qfv = new ArrayList<QueryFieldValue>();

			String qOrder = service.Parameter("orderBy");
			String qGroupBy = service.Parameter("groupBy");
			boolean bDistinct = new Boolean(service.Parameter("distinct"));
			String qFields = service.Parameter("columnList");
			String sFields = service.Parameter("selectList");
			String qFrom = service.Parameter("tableName");
			String qWhere = service.Parameter("where");

			if(null != qFrom && qFrom.contains(",")) {
				froms = qFrom.split(",");
			}else {
				if(null != service.Parameter("tableName")) {
					froms = new String[] {service.Parameter("tableName")};
				}else {
					froms = new String[] {"duel"};
				}
				
			}
			String timeStampFunctionName = "TIMESTAMP";
			int i = 0;
			for(i = 0;i<froms.length;i++){
				ArrayList<String> getM = null;
				TableMetaData metric = dbmd.getTables().get(froms[i]);
				
				String xTable = "";
				boolean pk = false;
				HashMap<String, ColumnMetaData> TableFields = null;

				if(vendorName.contains("oracle")){
					timeStampFunctionName = "TO_TIMESTAMP";
				}else if(vendorName.contains("sql server")){
					timeStampFunctionName = "";
				}

				xTable = froms[i];
				if(null != SchemaName && SchemaName.trim().length()>0){
					tables.append(SchemaName+"."+froms[i]);
				}else{
					tables.append(froms[i]);
				}
				TableFields = metric.getColumns();//.get(froms[i]).getColumnMetaData();
				if(i > 0)
					tables.append(",");

				if(null != qFields ){
					getM = (ArrayList<String>) Arrays.asList(qFields.split(","));
				}else{
					getM = metric.getFieldNames();
				}

				//{NONE_UNIQUE=true, DECIMAL_DIGITS=, KEY=true, KEYTABLENAME=cru_components, TYPE_NAME=VARCHAR, INDEX_NAME=Index 3, IS_AUTOINCREMENT=NO, NULLABLE=1, COLUMN_SIZE=240, COLUMN_NAME=COMPONENTNAME, COLUMN_DEF=}, {NONE_UNIQUE=false, DECIMAL_DIGITS=, KEY=true, KEYTABLENAME=cru_components, TYPE_NAME=VARCHAR, INDEX_NAME=PRIMARY, IS_AUTOINCREMENT=NO, NULLABLE=0, COLUMN_SIZE=40, COLUMN_NAME=COMPONENTINDEX, COLUMN_DEF=}, {NONE_UNIQUE=true, DECIMAL_DIGITS=, KEY=true, KEYTABLENAME=cru_components, TYPE_NAME=VARCHAR, INDEX_NAME=Index 2, IS_AUTOINCREMENT=NO, NULLABLE=1, COLUMN_SIZE=40, COLUMN_NAME=COMPONENTPARENTINDEX, COLUMN_DEF=}, {DECIMAL_DIGITS=, IS_AUTOINCREMENT=NO, COLUMN_DEF=1, NULLABLE=1, TYPE_NAME=INT, COLUMN_NAME=COMPONENTENABLED, COLUMN_SIZE=10}]"

				for(int g=0;g<getM.size();g++){

					String strF = null;
					String type = null;
					String CoreType = null;
					strF = TableFields.get(getM.get(g)).getColumnInfo().get("COLUMN_NAME");
					type = TableFields.get(getM.get(g)).getColumnInfo().get("TYPE_NAME");
					CoreType = TableFields.get(getM.get(g)).getColumnInfo().get("TYPE_NAME");
					pk = false;
					if(TableFields.get(getM.get(g)).getColumnInfo().containsKey("PrimaryKey")) {
						if(TableFields.get(getM.get(g)).getColumnInfo().get("PrimaryKey").equalsIgnoreCase("true")) {
							pk = true;
						}
					}

					String fieldName = xTable+"."+strF;
					String fieldValue = null;
					boolean excludeFromWhere = false;//obj.excludeFromWhere(fieldName);
					if( null != CoreType  && (CoreType.equalsIgnoreCase("CLOB")|| CoreType.equalsIgnoreCase("BLOB"))) {
						excludeFromWhere = true;
					}
					if(keys.containsKey(strF)){
						qfv.add(new QueryFieldValue((keys.get(strF)+"."+strF), fieldName, type, excludeFromWhere));
					}else{
						try{
							fieldValue = service.Parameter(strF);
							if(null != fieldValue && type.equalsIgnoreCase("boolean")){
								if(fieldValue.equals("false")){
									fieldValue = "0";
								}else{
									fieldValue = "1";
								}
							}
							keys.put(strF, xTable);
							qfv.add(new QueryFieldValue(fieldName, fieldValue, type, excludeFromWhere));
						}catch(Exception e){

						}
					}
				}
			}
			if(qfv.size()>0){
				for(int f=0;f<qfv.size();f++){
					String fv = qfv.get(f).getFieldValue();
					String fn = qfv.get(f).getFieldName();
					String ty = qfv.get(f).getFieldType();
					String fvFinished = valueCleanup(fn,fv,ty);
					boolean ts = false;
					if(null != fv && fv.trim().equalsIgnoreCase("null")) {
						fv = null;
					}
					if(ty.equalsIgnoreCase("TIMESTAMP") || ty.equalsIgnoreCase("DATE")){
						ts = true;
						/*if(null == fv){
							if(fn.contains(".")){
								fv = service.Parameter(fn).split("\\.")[1];
								if(null == fv || fv.trim().length()<1){
									fv = null;
								}
							}
						}*/
					}
					//if(DoNotExecuteQueries == false && (ty.equalsIgnoreCase("CLOB") || ty.equalsIgnoreCase("BLOB"))){
					//	fv = null;
					//}
					if(fields.toString().length()>0)
						fields.append(",");
					fields.append(fn);
					if(null != fv){
						if(ts && Character.isDigit(fv.charAt(1))){
							fv = fv.replaceAll("\'", "").trim();
							if(fv.length()>0){
								if(fv.trim().length()<11 && fv.contains(".") == false){
									fv = fv.trim()+" 00:00:00";
								}
								try{
									java.sql.Timestamp tsd = java.sql.Timestamp.valueOf(fv);
									if(vendorName.contains("oracle")){
										fv ="'"+tsd+"', '"+dataFormat+"'";
									}else if(vendorName.contains("sql server")){
										fv ="'"+fv+"'";
									}else{
										fv = "'"+tsd+"'";
									}
								}catch(Exception e){
									if(fv.contains(".")){

									}else {
										fv ="'"+fv+"'";
									}
								}

							}else{
								fv = "''";
							}
						}
						
						if(qfv.get(f).isExcludeFromWhere() == false){
							if(where.toString().length()>0)
								where.append(" and ");
							if(fv.equalsIgnoreCase("'<>null'"))
								fv = "is Not NULL";
							else if(fv.equalsIgnoreCase("'null'") || fv.equalsIgnoreCase("'~null'"))
								fv = "is NULL";
							if(fv.contains("||") && fv.contains("%")){
								StringTokenizer st = new StringTokenizer(fv.replaceAll("'",""),"||");
								//int cnt = st.countTokens();
								
								ArrayList<String> inList = new ArrayList<String>();
								ArrayList<String> partList = new ArrayList<String>();
								String tVal = null;
								while(st.hasMoreTokens()){
									tVal = (String)st.nextElement();
								    if(tVal.contains("%")){
								    	partList.add(tVal);
								    }else{
								    	inList.add(tVal);
								    }
								}
								
								where.append(" (");
								for(int l=0;l<inList.size();l++){
									if(l==0){
										where.append(fn+" in (");
									}else{
										where.append(",");
									}
									if(ts){
										if(vendorName.contains("oracle")){
											where.append("("+timeStampFunctionName+" ('"+inList.get(l)+"', '"+dataFormat+"')");
										}else{
											where.append("("+timeStampFunctionName+" ('"+inList.get(l)+"')");
										}
										
									}else{
										where.append("'"+inList.get(l)+"'");
									}
								}
								if(inList.size()>0){
									where.append(")");
								}
								for(int l=0;l<partList.size();l++){
									if(inList.size()>0 || l > 0 ){
										where.append(" or ");
									}
									where.append("upper("+fn+") like upper('"+partList.get(l)+"')");
								}
								where.append(" ) ");
								
							}else if(fv.contains("||")){
									StringTokenizer st = new StringTokenizer(fv,"||");
									int cnt = st.countTokens();
									where.append(fn+" in (");
									while(st.hasMoreTokens()){
										if(ts){
											if(vendorName.contains("oracle")){
												where.append("("+timeStampFunctionName+" ('"+(String)st.nextElement()+"', '"+dataFormat+"')");
											}else{
												where.append("("+timeStampFunctionName+" ('"+(String)st.nextElement()+"')");
											}
											
										}else{
											where.append((String)st.nextElement());
										}
										
										if(cnt > 1){
											where.append("','");
										}
										--cnt;
									}
									where.append(")");
							}else if(fv.contains("%")){
								where.append("upper("+fn+") like upper("+fvFinished+")");
							}else if(fv.startsWith("^") || fv.contains("'^")){
								where.append("upper("+fn+") = upper("+fvFinished.replace("^","").toUpperCase()+")");
							}else{
								/*
								> -- greater than
								< -- less than
								>= -- greater than or equal to
								<= -- less than or equal to
								<> -- not equal to or !=
								 */
								if(fv.startsWith("'<>"))
									if(ts){
										where.append(fn+" <> ("+timeStampFunctionName+"  "+"("+fvFinished+"))");
									}else{
										where.append(fn+" <> "+"'"+fv.substring(3));
									}
									
								else if(fv.startsWith("<>"))
									if(ts){
										where.append(fn+" <> ("+timeStampFunctionName+"  ("+fvFinished+"))");
									}else{
										where.append(fn+" <> "+fv.substring(2));
									}
								// start of change
								else if(fv.startsWith("'!="))
									if(ts){
										where.append(fn+" <> ("+timeStampFunctionName+"  "+"("+fvFinished+"))");
									}else{
										where.append(fn+" <> "+"'"+fv.substring(3));
									}
									
								else if(fv.startsWith("!="))
									if(ts){
										where.append(fn+" <> ("+timeStampFunctionName+"  ("+fvFinished+"))");
									}else{
										where.append(fn+" <> "+fv.substring(2));
									}
								// end of change
								else if(fv.startsWith("'>&"))
									if(ts){
										where.append(fn+" >= ("+timeStampFunctionName+"  "+"("+fvFinished+"))");
									}else{
										where.append(fn+" >= "+"'"+fv.substring(3));
									}
								
								else if(fv.startsWith(">&"))
									if(ts){
										where.append(fn+" >= ("+timeStampFunctionName+"  ("+fv+"))");
									}else{
										where.append(fn+" >= "+fv.substring(2));
									}
	
								else if(fv.startsWith("'<&"))
									if(ts){
										where.append(fn+" <= ("+timeStampFunctionName+"  "+"("+fv+"))");
									}else{
										where.append(fn+"<= "+"'"+fv.substring(3));
									}
								
								else if(fv.startsWith("<&"))
									if(ts){
										where.append(fn+" <= ("+timeStampFunctionName+"  ("+fv+"))");
									}else{
										where.append(fn+" <= "+fv.substring(2));
									}
	
								else if(fv.startsWith("'>"))
									if(ts){
										where.append(fn+" > ("+timeStampFunctionName+"  "+"("+fv+"))");
									}else{
										where.append(fn+" > "+"'"+fv.substring(2));
									}
									
								else if(fv.toLowerCase().startsWith("is null"))
									if(ts){
										where.append(fn+" is NULL");
									}else{
										where.append(fn+" is NULL");
									}
	
								else if(fv.toLowerCase().startsWith("'is null"))
									if(ts){
										where.append(fn+"  is NULL");
									}else{
										where.append(fn+" is NULL");
									}		
								else if(fv.toLowerCase().startsWith("is not null"))
									if(ts){
										where.append(fn+" is not NULL");
									}else{
										where.append(fn+" is not NULL");
									}
	
								else if(fv.toLowerCase().startsWith("'is not null"))
									if(ts){
										where.append(fn+"  is not NULL");
									}else{
										where.append(fn+" is not NULL");
									}								
								else if(fv.startsWith(">"))
									if(ts){
										where.append(fn+" > ("+timeStampFunctionName+"  ("+fv+"))");
									}else{
										where.append(fn+" > "+fv.substring(1));
									}
	
								else if(fv.startsWith("'<"))
									if(ts){
										where.append(fn+" < ("+timeStampFunctionName+"  "+"("+fv+"))");
									}else{
										where.append(fn+" < "+"'"+fv.substring(2));
									}
								
								else if(fv.startsWith("<"))
									if(ts){
										where.append(fn+" < ("+timeStampFunctionName+"  ("+fv+"))");
									}else{
										where.append(fn+" < "+fv.substring(1));
									}
								else if(fv.startsWith("#"))
									if(ts){
										String[] split = fv.split(",");
										//where.append(" TRUNC(TO_TIMESTAMP("+fn+",'YYYY-MM-dd')) BETWEEN TRUNC(TO_TIMESTAMP('"+split[0].substring(1)+"','YYYY-MM-dd')) AND TRUNC(TO_TIMESTAMP('"+split[1]+"','YYYY-MM-dd'))");
										where.append(" ("+fn+" >= (TO_TIMESTAMP('"+split[0].substring(1).trim()+"','YYYY-MM-dd')) AND "+fn+" <= (TO_TIMESTAMP('"+split[1].trim()+"','YYYY-MM-dd'))) ");
									}else{
										
									}	
								else
									if(ts){
										where.append(fn+"=("+timeStampFunctionName+"  ("+fvFinished+"))");
									}else{
										if(!fvFinished.startsWith("'"))
											fvFinished = "'"+fvFinished;
										if(!fvFinished.endsWith("'"))
											fvFinished = fvFinished+"'";
										where.append(fn+"="+fvFinished+"");
									}
							}
						}
					}
				}
			}
			sbQCount.append("Select count(*) as TOTALRECORDS ");
			sbQ.append("Select ");
			if(bDistinct)
				sbQ.append("DISTINCT ");
			if(null != qFields ){
				sbQ.append(qFields.toString());
			}else if(null != sFields){
				sbQ.append(sFields);
			}
			else{
				if(bAll)
					sbQ.append("*");//from "+tables.toString());
				else
					sbQ.append(fields.toString());//+" from "+tables.toString());
			}
			if(null != qFrom){
				sbQ.append(" from "+qFrom);
				sbQCount.append(" from "+qFrom);
			}else{
				sbQ.append(" from "+tables.toString());
				sbQCount.append(" from "+tables.toString());
			}
			if(null != qWhere){
				if(qWhere.trim().toLowerCase().startsWith("and ")){
					if(where.length()>0){
						sbQ.append(" where "+where.toString()+" "+qWhere.trim());
						sbQCount.append(" where "+where.toString()+" "+qWhere.trim());
					}else{
						sbQ.append(" where "+qWhere.substring(4));
						sbQCount.append(" where "+where.toString()+" "+qWhere.trim());
					}
				}else{
					sbQ.append(" where "+qWhere);
					sbQCount.append(" where "+qWhere);
				}
			}else{
				if(where.length()>0){
					sbQ.append(" where "+where.toString());
					sbQCount.append(" where "+where.toString());
				}
			}
			if(null != qGroupBy && qGroupBy.length()>0){
				sbQ.append(" Group By "+qGroupBy.toString());
				sbQCount.append(" Group By "+qGroupBy.toString());
			}
			if(null != qOrder && qOrder.length()>0){
				sbQ.append(" Order By "+qOrder.toString());
			}
			return sbQ.toString();
			// throws SQLException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, Exception
		}catch(Exception err){
			throw err;
		}

	}
	
	public static String createInsert(SessionObject so, Services service) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, Exception{
		//if(fwSess.isPerformanceMonitor()){
		//	MsgLog.state(fwSess, "PERFORMANCE", "<tr><td align='right' width='30%'>"+Long.valueOf(System.currentTimeMillis()).toString()+":SqlDOManager CreateInsert Start");
		//}
		try{
			DBMetaData dbmd = cruConnection.getPoolMetaData(so, service);
			if(null == dbmd) {
				return null;
			}
			String vendorName = dbmd.getVendor();
			String SchemaName = dbmd.getSchema();
			StringBuffer sbQ 	= new StringBuffer();
			StringBuffer fields = new StringBuffer();
			StringBuffer tables = new StringBuffer();
			String[] froms = null;
			HashMap<String, String> keys 		= new HashMap<String, String>();
			ArrayList<QueryFieldValue> qfv = new ArrayList<QueryFieldValue>();
			String xTable = "";
			String qFields = service.Parameter("columnList");
			String qFrom = service.Parameter("tableName");
			boolean pk = false;
			if(null != qFrom && qFrom.contains(",")) {
				froms = qFrom.split(",");
			}else {
				froms = new String[] {qFrom};
			}
			String timeStampFunctionName = "TIMESTAMP";
			int i = 0;
			for(i = 0;i<froms.length;i++){
				ArrayList<String> getM = null;
				TableMetaData metric = dbmd.getTables().get(froms[i]);
				
				pk = false;
				HashMap<String, ColumnMetaData> TableFields = null;

				if(vendorName.contains("oracle")){
					timeStampFunctionName = "TO_TIMESTAMP";
				}else if(vendorName.contains("sql server")){
					timeStampFunctionName = "";
				}

				xTable = froms[i];
				if(null != SchemaName && SchemaName.trim().length()>0){
					tables.append(SchemaName+"."+froms[i]);
				}else{
					tables.append(froms[i]);
				}
				TableFields = metric.getColumns();//.get(froms[i]).getColumnMetaData();
				if(i > 0)
					tables.append(",");

				if(null != qFields ){
					getM = (ArrayList<String>) Arrays.asList(qFields.split(","));
				}else{
					getM = metric.getFieldNames();
				}

				for(int g=0;g<getM.size();g++){
					String strF = null;
					String type = null;
					String CoreType = null;
					strF = TableFields.get(getM.get(g)).getColumnInfo().get("COLUMN_NAME");
					type = TableFields.get(getM.get(g)).getColumnInfo().get("TYPE_NAME");
					CoreType = TableFields.get(getM.get(g)).getColumnInfo().get("TYPE_NAME");
					pk = false;
					if(TableFields.get(getM.get(g)).getColumnInfo().containsKey("PrimaryKey")) {
						if(TableFields.get(getM.get(g)).getColumnInfo().get("PrimaryKey").equalsIgnoreCase("true")) {
							pk = true;
						}
					}
					String fieldName = xTable+"."+strF;
					String fieldValue = null;
					boolean excludeFromWhere = false;//obj.excludeFromWhere(fieldName);
					if( null != CoreType  && (CoreType.equalsIgnoreCase("CLOB")|| CoreType.equalsIgnoreCase("BLOB"))) {
						excludeFromWhere = true;
					}
					if(keys.containsKey(strF)){
						qfv.add(new QueryFieldValue((keys.get(strF)+"."+strF), fieldName, type, excludeFromWhere));
					}else{
						try{
							fieldValue = service.Parameter(strF);
							if(null != fieldValue && type.equalsIgnoreCase("boolean")){
								if(fieldValue.equals("false")){
									fieldValue = "0";
								}else{
									fieldValue = "1";
								}
							}
							keys.put(strF, xTable);
							qfv.add(new QueryFieldValue(fieldName, fieldValue, type, excludeFromWhere));
						}catch(Exception e){

						}
					}
				}
			}
			StringBuffer values 	= new StringBuffer();
			if(qfv.size()>0){
				for(int f=0;f<qfv.size();f++){
					String fv = qfv.get(f).getFieldValue();
					String fn = qfv.get(f).getFieldName();
					String ty = qfv.get(f).getFieldType();
					//String fvFinished = valueCleanup(fn,fv,ty);
					if(!qfv.get(f).isExcludeFromWhere()){
						if(null != fv){
							if(fields.toString().length()>0)
								fields.append(",");
							fields.append(fn);
							if(values.toString().length()>0)
								values.append(",\n");
							
							if(ty.equalsIgnoreCase("timestamp") ||ty.equalsIgnoreCase("date")){
								if(qfv.get(f).getFieldType().equalsIgnoreCase("date")){
									java.sql.Date tsdd = java.sql.Date.valueOf(fv.replaceAll("\'", ""));
									if(vendorName.contains("oracle")){
										values.append("("+timeStampFunctionName+"  ('"+tsdd+"','"+dataFormat+"'))");
									}else if(vendorName.contains("sql server")){
										values.append(fv);
									}else{
										values.append("("+timeStampFunctionName+"  ('"+tsdd+"'))");
									}
								}else{
									java.sql.Timestamp tsdt = java.sql.Timestamp.valueOf(fv.replaceAll("\'", ""));
									if(vendorName.contains("oracle")){
										values.append("("+timeStampFunctionName+"  ('"+tsdt+"','"+dataFormat+"'))");
									}else if(vendorName.contains("sql server")){
										values.append(fv);
									}else{
										values.append("("+timeStampFunctionName+"  ('"+tsdt+"'))");
									}
								}
							}else if( (qfv.get(f).getFieldType().equalsIgnoreCase("clob") ||ty.equalsIgnoreCase("blob"))){
								if(vendorName.contains("oracle")){
									values.append("?");

								}else{
									values.append(fv);
								}//(to_clob
							}else if(fv.length()> 3000 && vendorName.contains("oracle") && (qfv.get(f).getFieldType().equalsIgnoreCase("clob") ||ty.equalsIgnoreCase("blob"))){
								values.append(splitClob(fv.substring(1, fv.length()-1), 3000));
							}else{
								if(!fv.startsWith("'"))
									fv = "'"+fv;
								if(!fv.endsWith("'"))
									fv = fv+"'";
								
								
								values.append(fv);
							}
						}
					}
				}
			}
			//update table
			sbQ.append("insert into ");
			if(null != SchemaName && SchemaName.trim().length()>0){
				sbQ.append(" "+SchemaName+"."+xTable+ " ("+fields.toString()+") values ("+values+")");
			}else{
				sbQ.append(" "+xTable+ " ("+fields.toString()+") values ("+values+")");
			}
			
			//if(fwSess.isPerformanceMonitor()){
			//	MsgLog.state(fwSess, "PERFORMANCE", "<tr><td align='right' width='30%'>"+Long.valueOf(System.currentTimeMillis()).toString()+":SqlDOManager CreateInsert End");
			//}
			return sbQ.toString();

		}catch(IllegalAccessException err){
			throw err;
		}catch(IllegalArgumentException err){
			throw err;
		}catch(InvocationTargetException err){
			throw err;
		}catch(Exception err){
			throw err;
		}

	}
	public static String createDelete(SessionObject so, Services service) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, Exception{
		//if(fwSess.isPerformanceMonitor()){
		//	MsgLog.state(fwSess, "PERFORMANCE", "<tr><td align='right' width='30%'>"+Long.valueOf(System.currentTimeMillis()).toString()+":SqlDOManager CreateInsert Start");
		//}
		try{
			DBMetaData dbmd = cruConnection.getPoolMetaData(so, service);
			if(null == dbmd) {
				return null;
			}
			String vendorName = dbmd.getVendor();
			String SchemaName = dbmd.getSchema();
			StringBuffer sbQ 	= new StringBuffer();
			StringBuffer fields = new StringBuffer();
			StringBuffer tables = new StringBuffer();
			String[] froms = null;
			HashMap<String, String> keys 		= new HashMap<String, String>();
			ArrayList<QueryFieldValue> qfv = new ArrayList<QueryFieldValue>();
			String xTable = "";
			String qFields = service.Parameter("columnList");
			String qFrom = service.Parameter("tableName");

			if(null != qFrom && qFrom.contains(",")) {
				froms = qFrom.split(",");
			}else {
				froms = new String[] {qFrom};
			}
			String timeStampFunctionName = "TIMESTAMP";
			int i = 0;
			for(i = 0;i<froms.length;i++){
				ArrayList<String> getM = null;
				TableMetaData metric = dbmd.getTables().get(froms[i]);
				
				boolean pk = false;
				HashMap<String, ColumnMetaData> TableFields = null;

				if(vendorName.contains("oracle")){
					timeStampFunctionName = "TO_TIMESTAMP";
				}else if(vendorName.contains("sql server")){
					timeStampFunctionName = "";
				}

				xTable = froms[i];
				if(null != SchemaName && SchemaName.trim().length()>0){
					tables.append(SchemaName+"."+froms[i]);
				}else{
					tables.append(froms[i]);
				}
				TableFields = metric.getColumns();//.get(froms[i]).getColumnMetaData();
				if(i > 0)
					tables.append(",");

				if(null != qFields ){
					getM = (ArrayList<String>) Arrays.asList(qFields.split(","));
				}else{
					getM = metric.getFieldNames();
				}

				for(int g=0;g<getM.size();g++){
					String strF = null;
					String type = null;
					String CoreType = null;
					strF = TableFields.get(getM.get(g)).getColumnInfo().get("COLUMN_NAME");
					type = TableFields.get(getM.get(g)).getColumnInfo().get("TYPE_NAME");
					CoreType = TableFields.get(getM.get(g)).getColumnInfo().get("TYPE_NAME");
					pk = false;
					if(TableFields.get(getM.get(g)).getColumnInfo().containsKey("PrimaryKey")) {
						if(TableFields.get(getM.get(g)).getColumnInfo().get("PrimaryKey").equalsIgnoreCase("true")) {
							pk = true;
						}
					}
					String fieldName = xTable+"."+strF;
					String fieldValue = null;
					boolean excludeFromWhere = false;//obj.excludeFromWhere(fieldName);
					if( null != CoreType  && (CoreType.equalsIgnoreCase("CLOB")|| CoreType.equalsIgnoreCase("BLOB"))) {
						excludeFromWhere = true;
					}
					if(keys.containsKey(strF)){
						qfv.add(new QueryFieldValue((keys.get(strF)+"."+strF), fieldName, type, excludeFromWhere));
					}else{
						try{
							fieldValue = service.Parameter(strF);
							if(null != fieldValue && type.equalsIgnoreCase("boolean")){
								if(fieldValue.equals("false")){
									fieldValue = "0";
								}else{
									fieldValue = "1";
								}
							}
							keys.put(strF, xTable);
							qfv.add(new QueryFieldValue(fieldName, fieldValue, type, excludeFromWhere));
						}catch(Exception e){

						}
					}
				}
			}
			StringBuffer values 	= new StringBuffer();
			StringBuffer where 	= new StringBuffer();
			if(qfv.size()>0){
				for(int f=0;f<qfv.size();f++){
					String fv = qfv.get(f).getFieldValue();
					String fn = qfv.get(f).getFieldName();
					String ty = qfv.get(f).getFieldType();
                    if(null != fv) {
						if(!fv.startsWith("'"))
							fv = "'"+fv;
						if(!fv.endsWith("'"))
							fv = fv+"'";
                    }
					
					
					if(!qfv.get(f).isExcludeFromWhere()){
						if((null != fv)){
							if(where.toString().length()>0)
								where.append(" and ");
							where.append(fn+"="+fv);
						}
					}


				}
			}
			//update table
			sbQ.append("delete from ");
			if(null != SchemaName && SchemaName.trim().length()>0){
				sbQ.append(" "+SchemaName+"."+tables+" ");
			}else{
				sbQ.append(" "+tables+" ");
			}

			if(where.length()>0) {
				sbQ.append(" where "+where.toString());
			}else {
				Clog.Error(so, "service", "900001", "Problem with Delete, no qualifying where clause:"+sbQ.toString());
				sbQ = new StringBuffer();
			}
			
			
			return sbQ.toString();
			/*if(metric.getCurrent().isQueryOk(Query) && DoNotExecuteQueries == false){
				size=0;
				try {
					DBStore.execute(this,tables);
				} catch (Exception e) {
					throw e;
				}
			}*/

		}catch(Exception err){
			throw err;
		}
	}
	
	
	private static String splitClob(String x, int size) throws Exception{
		StringBuffer sbClob = new StringBuffer();
		sbClob.append("(to_clob('");
		InputStream is = new ByteArrayInputStream(x.getBytes());
		// read it with BufferedReader
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		int i = 0;
		int cnt = 0;
		char cc = 0;
		try{
			while ((i = br.read()) != -1) {
				++cnt;
				cc = (char)i;
				if(cnt > size && cc != '\''){
					sbClob.append("') || to_clob('");
					cnt=0;
				}
				if((i < 32 || i > 126) && ((i != 9) && (i != 10) && (i != 13))){
					//System.out.println("null special char");
					cc = ' ';
				}
				sbClob.append(cc);

			}
			sbClob.append("'))");
			br.close(); 
		}catch(Exception e){
			throw e;
		}
		return sbClob.toString();//.replaceAll(bell, "'");
	}	
	public static String getTypeConName(String TypeName, int size, int Dec){
		String[] sp = TypeName.split(" ");
		if(sp.length>1)
			TypeName = sp[0].trim();
		sp = TypeName.split("\\(");
		if(sp.length>1)
			TypeName = sp[0].trim();
		String sReturn = "String";
		if(TypeName.equalsIgnoreCase("NUMBER")){
			try{
				if(Dec == 0){
					sReturn =  BigDecimal.class.getSimpleName();
				}else if(Dec > 6){
					sReturn = BigDecimal.class.getSimpleName();
				}else{
					sReturn = (String) TypeConSimple.get(TypeName);
				}
			}catch(Exception err){
				err.printStackTrace();
				//MsgLog.error("GenUtil Exception:"+err.getMessage());
			}
		}else{
			sReturn =  (String) TypeConSimple.get(TypeName);
		}
		return sReturn;
	}
	public static String convertString(String sType , String sValue) throws Exception{
		//String sType = Args.getSimpleName();
		//int size = Args.getTypeParameters().length
		//String sTypeLong = Args.getName();
		Object o = new Object[1];
		boolean emptyToNull = true;//DBProps.isEmptyNullNumbers();
		boolean Empty = false;
		if(null ==sValue || sValue.trim().length()<1) {
			Empty = true;
			o = "";
		}else {
			try{
				if(	sType.equalsIgnoreCase("BIT")  || sType.equalsIgnoreCase("BOOL") || sType.equalsIgnoreCase("BOOLEAN"))
					if(Empty || null == sValue || sValue.equals("0")  || sValue.equalsIgnoreCase("false")){
						o 			= new Boolean(false);
					}else{
						o			= new Boolean(true);
					}
				else if(sType.equalsIgnoreCase("INTEGER") || sType.equalsIgnoreCase("SMALLINT") || sType.equalsIgnoreCase("INT") || sType.equalsIgnoreCase("MEDIUMINT") || sType.equalsIgnoreCase("TINYINT"))
					if(Empty){
						if(emptyToNull){
							o = null;
						}else{
							o 			= new Integer(0);
						}
					}else
						o			= new Integer(sValue);
				else if(sType.equalsIgnoreCase("REAL") || sType.equalsIgnoreCase("FLOAT"))
					if(Empty){
						if(emptyToNull){
							o = null;
						}else{
							o 			= new Float(0);
						}
					}else
						o			= new Float(sValue);
				else if(sType.equalsIgnoreCase("BIGINT")||sType.equalsIgnoreCase("LONG"))
					if(Empty){
						if(emptyToNull){
							o = null;
						}else{
							o 			= new Long(0);
						}
					}else
						o			= new Long(sValue);
				else if(sType.equalsIgnoreCase("NUMBER"))
					if(Empty){
						if(emptyToNull){
							o = null;
						}else{
							o 			= new BigDecimal(0);
						}
					}else
						o			= new BigDecimal(sValue);
				else if(sType.equalsIgnoreCase("BIGDECIMAL"))
					if(Empty){
						if(emptyToNull){
							o = null;
						}else{
							o 			= new BigDecimal(0);
						}
					}else
						o			= new BigDecimal(sValue);
				else if(sType.equalsIgnoreCase("SHORT"))
					if(Empty){
						if(emptyToNull){
							o = null;
						}else{
							o 			= new Short((short)0);
						}
					}else {
						o			= new Short(sValue);
					}
				else if(sType.equalsIgnoreCase("DOUBLE"))
					if(Empty){
						if(emptyToNull){
							o = null;
						}else{
							o 			= new Double(0);
						}
					}else {
						o			= new Double(sValue);
					}
				else if(sType.equalsIgnoreCase("BYTE"))
					if(Empty) {
						o			= new Byte((byte)0);
					}else {
						o			= new Byte(sValue);
					}
				else if(sType.equalsIgnoreCase("DATE") && (null != sValue) && (sValue.trim().length()>6)) {
					if(Empty) {
						o 			=  null;
					}else {
						try{
							//if(sTypeLong.trim().equalsIgnoreCase("java.sql.Date")){
							if(sValue.trim().length()>10){
								sValue 		= sValue.substring(0,11).trim();
							}
							o	= java.sql.Date.valueOf(sValue);
							//}else{
							//	o	= java.sql.Date.valueOf(sValue);
							//}
						}catch(Exception e){
							o		= (sValue);
						}
					}
				}else if(sType.equalsIgnoreCase("DATETIME") && (null != sValue) && (sValue.trim().length()>6)) {
					if(Empty)
						o 			=  null;
					else
						try{
							//if(sTypeLong.trim().equalsIgnoreCase("java.sql.Timestamp") || sTypeLong.trim().equalsIgnoreCase("java.sql.Date")){
							if(sValue.trim().length()>10){
								sValue 	= sValue.substring(0,11).trim();
							}
							o	= java.sql.Timestamp.valueOf(sValue);//java.sql.Date.valueOf(sValue);
							//}else{
							//	o	= java.sql.Timestamp.valueOf(sValue);// java.sql.Date.valueOf(sValue);
							//}
						}catch(Exception e){
							o		= (sValue);
						}
				}else if(sType.equalsIgnoreCase("TIME") && (null != sValue) && (sValue.trim().length()>6) ) {
					if(Empty)
						o 			= null;
					else
						o			= java.sql.Time.valueOf(sValue);
				}else if(sType.equalsIgnoreCase("TIMESTAMP") && (null != sValue)) {
					if(Empty) {
						o 			= null;
					}else if((sValue.trim().length()>8)) {
						try{
							o	= java.sql.Timestamp.valueOf(sValue);
						}catch(Exception e){
							//System.out.println(sValue);
							try{
								String[] dParts = sValue.split(" ");
								if(dParts.length > 2){
									o	= java.sql.Timestamp.valueOf(dParts[0]+" "+dParts[1]);
								}
							}catch(Exception er){
								throw er;
							}
							//e.printStackTrace();
						}
					}else {
						o = null;
					}
				}else {
					o				= "'"+sValue+"'";
				}
			}catch(Exception err){
				throw err;
			}
		}
		return o.toString();
	}
	public final static HashMap<Object, Object> TypeCon = new HashMap<Object, Object>(){/**
		 * 
		 */
		private static final long serialVersionUID = -6678299526496611923L;

	{
		put("BIT", Boolean.class);
		put("CHAR", String.class);
		put("NCHAR", String.class);
		put("TINYINT", Byte.class);
		put("INT", Integer.class);
		put("SMALLINT", Integer.class);
		put("INTEGER", Integer.class);
		put("NUMBER", BigDecimal.class);//BigDecimal
		put("NUMERIC", BigDecimal.class);
		put("BIGINT", Long.class);
		put("REAL", Float.class);
		put("FLOAT", Float.class);
		put("DOUBLE", Double.class);
		put("DECIMAL", BigDecimal.class);
		put("BIGDECIMAL", BigDecimal.class);
		put("MONEY", Double.class);
		put("DATE", java.sql.Date.class);
		put("DATETIME", java.sql.Timestamp.class);
		put("TIME", java.sql.Time.class);
		put("SHORT", Short.class);
		put("TIMESTAMP", java.sql.Timestamp.class);
		put("VARCHAR", String.class);
		put("VARCHAR2", String.class);
		put("NVARCHAR2", String.class);
		put("NVARCHAR", String.class);
		put("UNIQUEIDENTIFIER", String.class);
		put("ENUM", String.class);
		put("SET", String.class);
		put("YEAR", Short.class);
		put("BLOB", String.class);
		put("CLOB", String.class);
		put("BINARY", String.class);
		put("VARBINARY", String.class);
		put("SMALLDATETIME", java.sql.Timestamp.class);
		put("XML", String.class);
		put("MONEY", BigDecimal.class);
		put("SMALLMONEY",BigDecimal.class);
		put("DATETIME2", java.sql.Timestamp.class);
		
	}};
	public final static HashMap<Object, Object> TypeConSimple = new HashMap<Object, Object>(){/**
		 * 
		 */
		private static final long serialVersionUID = 8896456567745819311L;

	{
		put("BIT", "Boolean");
		put("BOOLEAN", "Boolean");
		put("TINYINT", "Byte");
		put("INT", "Integer");
		put("SMALLINT", "Integer");
		put("MEDIUMINT", "Integer");
		put("SHORT", "Short");
		put("INTEGER", "Integer");
		put("NUMBER", "BigDecimal");
		put("NUMERIC", "BigDecimal");
		put("MONEY", "Double");
		put("BIGINT", "Long");
		put("REAL", "Float");
		put("FLOAT", "Float");
		put("DOUBLE", "Double");
		put("BIGDECIMAL", "BigDecimal");
		put("DATE", "Date");
		put("DATETIME", "Timestamp");
		put("TIME", "Time");
		put("TIMESTAMP", "Timestamp");
		put("NVARCHAR", "String");
		put("NVARCHAR2", "String");
		put("VARCHAR", "String");
		put("VARCHAR2", "String");
		put("UNIQUEIDENTIFIER", "String");
		put("RAW", "String");
		put("TEXT", "String");
		put("CHAR", "String");
		put("NCHAR", "String");
		put("SMALLTEXT", "String");
		put("LARGETEXT", "String");
		put("MEDIUMTEXT", "String");
		put("ENUM", "String");
		put("SET", "String");
		put("YEAR", "Short");
		put("BLOB", "String");
		put("MEDIUMBLOB", "String");
		put("LONGBLOB", "String");
		put("CLOB", "Clob");
		put("DECIMAL", "BigDecimal");
		put("BINARY", "Binary");
		put("VARBINARY", "Binary");
		put("SMALLDATETIME", "Timestamp");
		put("XML", "String");
		put("MONEY", "BigDecimal");
		put("SMALLMONEY", "BigDecimal");
		put("DATETIME2", "Timestamp");
		put("TINYTEXT", "String");
		put("LONGTEXT", "String");
		
		
	}};

}

