package com.cruise.plugins.CruiseDatabase;

import java.sql.ResultSet;

import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.interfaces.PluginInterface;
import com.corecruise.cruise.services.utils.Services;
import com.corecruise.cruise.services.utils.GenericSessionResp;
import com.cruise.plugins.Action;
import com.cruise.plugins.ActionParameter;
import com.cruise.plugins.PlugInMetaData;
import com.cruise.plugins.CruiseDatabase.metadata.DBMetaData;
import com.cruise.plugins.CruiseDatabase.metadata.TableMetaData;
import com.cruise.plugins.CruiseDatabase.utils.JsonNodeRowMapper;
import com.cruise.plugins.CruiseDatabase.utils.QueryBuilder;
import com.cruise.plugins.CruiseDatabase.utils.cruConnection;
import com.cruise.plugins.CruiseDatabase.utils.cruConnectionObject;


/**
 * Hello world!
 *
 */
public class CruiseDatabase implements PluginInterface
{


	PlugInMetaData pmd = null;
    public CruiseDatabase() {
    	pmd = new PlugInMetaData("CruiseDatabase","0.0.1","SJC","Database access plugin");
    	
    	pmd.getActions().add(new Action("info", "getPlugin Information"));
    	pmd.getActions().get(0).getActionParams().add(new ActionParameter("None","false","unknown","Unused Parameter"));
    	
    	pmd.getActions().add(new Action("CruiseTest", "Test API Call"));
		pmd.getActions().get(1).getActionParams().add(new ActionParameter("Sample","false","unknown","Unused Parameter"));
		
    	pmd.getActions().add(new Action("PlugInInfo", "get information about the pluging"));
    	pmd.getActions().get(2).getActionParams().add(new ActionParameter("Sample","false","unknown","Unused Parameter"));

    	pmd.getActions().add(new Action("cDBCreatePool", "Create a new PooledConnection"));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("PoolName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("DriverClassName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("jdbcUrl","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("username","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("password","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("maximumPoolSize","false","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("minimumIdle","false","unknown","Name of the Connection pool. This name is used in subsequent calls."));
 
    	pmd.getActions().add(new Action("cDBGetPoolInfo", "Returns the pool information"));
		pmd.getActions().get(4).getActionParams().add(new ActionParameter("PoolName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));

    	pmd.getActions().add(new Action("cDBGetTableInfo", "Returns the Table information"));
		pmd.getActions().get(5).getActionParams().add(new ActionParameter("PoolName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(5).getActionParams().add(new ActionParameter("tablename","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));

    	pmd.getActions().add(new Action("select", "Queries a table"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("PoolName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
    	pmd.getActions().get(6).getActionParams().add(new ActionParameter("fromlist","true","Duel","Table name or list of tables names that make up the 'From' clause"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("selectlist","true","Duel","list of fields"));

    	pmd.getActions().add(new Action("update", "Queries a table"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("PoolName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
    	pmd.getActions().get(6).getActionParams().add(new ActionParameter("fromlist","true","Duel","Table name or list of tables names that make up the 'From' clause"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("selectlist","true","Duel","list of fields"));
		//pmd.getActions().get(6).getActionParams().add(new ActionParameter("selectlist","true","Duel","list of fields"));

		/*
			String qOrder = service.Parameter("OrderBy");
			String qGroupBy = service.Parameter("GroupBy");
			boolean bDistinct = new Boolean(service.Parameter("Distinct"));
			String qFields = service.Parameter("ColumnList");
			String sFields = service.Parameter("SelectList");
			String qFrom = service.Parameter("FromList");
			String qWhere = service.Parameter("Where"); 
		 */
    
    }

	public boolean executePlugin(SessionObject so, Services service) {
		boolean ret = false;
		String action = service.Action();
		GenericSessionResp gro = new GenericSessionResp();
		DBMetaData dbmd = null;
		cruConnectionObject cruConn = null;
		switch (action) {
		case "info":
			so.appendToResponse(pmd);
			ret = true;
			break;
		case "CruiseTest":
			gro.addParmeter("PluginEnabled", "true");
			so.appendToResponse("CruiseTest",gro);
			break;
		case "PlugInInfo":
			if(null != pmd) {
				so.appendToResponse("PlugInInfo", pmd);
				ret = true;
			}else {
				Clog.Error(so, "PlugInInfo", "100.01", "Failed to get any information about the plugin.");
			}
			break;
		case "cDBCreatePool":
			if(!cruConnection.isPoolLoaded(so, service)) {
				ret = cruConnection.createConnectionPool(so,service);
				gro.addParmeter("PoolCreated", new Boolean(ret).toString());
				so.appendToResponse(gro);
			}else {
				gro.addParmeter("Pool Already exists", "Not added.");
				so.appendToResponse("PoolCreation",gro);
				ret = true;
			}
			break;
		case "cDBGetPoolInfo":
			dbmd = cruConnection.getPoolMetaData(so, service);
			if(null != dbmd) {
				so.appendToResponse("PoolInfo", dbmd);
				ret = true;
			}else {
				Clog.Error(so, "GetPoolInfo", "100.01", "Failed to get any information about the pool");
			}
			break;
		case "cDBGetTableInfo":
			dbmd = cruConnection.getPoolMetaData(so, service);
			if(dbmd.getTables().containsKey(service.Parameter("TableName"))) {
				TableMetaData tmd = dbmd.getTables().get(service.Parameter("TableName"));
				so.appendToResponse("PoolInfo"+service.Parameter("TableName"), tmd);
				ret = true;
			}else {
				Clog.Error(so, "GetTableInfo", "100.02", "Failed to get any information about the pool");
			}
			break;
		case "cDBGetConnection":
			
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					so.setRequestState(service.Parameter("PoolName"), cruConn);
					ret = true;
				}else {
					Clog.Error(so, "ser", "100.03", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "100.04", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}finally {
				if(null != cruConn) {
					cruConn.Close();
				}
			}
			break;
		case "select":
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					String query = QueryBuilder.createSelect(so, service, false);
					gro.addParmeter("Query", query);
					so.appendToResponse(gro);
					ResultSet rs = cruConn.getConn().prepareStatement(query).executeQuery();
					if(null != rs) {
						JsonNodeRowMapper.mapRows(so, rs);
					}
					ret = true;
				}else {
					Clog.Error(so, "ser", "100.03", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "100.04", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}finally {
				if(null != cruConn) {
					cruConn.Close();
				}
			}
			break;
		case "update":
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					String query = QueryBuilder.createUpdate(so, service);
					gro.addParmeter("Query", query);
					so.appendToResponse(gro);
					Integer updated = cruConn.getConn().prepareStatement(query).executeUpdate();
					gro.addParmeter("UpdateCount", updated.toString());
					ret = true;
				}else {
					Clog.Error(so, "ser", "100.03", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "100.04", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}finally {
				if(null != cruConn) {
					cruConn.Close();
				}
			}
			break;
		default:
			Clog.Error(so, "service", "100.05", "Invalid Action supplied:"+action);
		}
		return ret;
	}

	@Override
	public PlugInMetaData getPlugInMetaData() {
		// TODO Auto-generated method stub
		return pmd;
	}

	@Override
	public void setPluginVendor(PlugInMetaData PMD) {
		pmd = PMD;
		
	}






}
