package com.cruise.plugins.CruiseDatabase;

import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.interfaces.PluginInterface;

import com.corecruise.cruise.services.utils.Services;
import com.cruise.plugins.Action;
import com.cruise.plugins.ActionParameter;
import com.cruise.plugins.PlugInMetaData;
import com.cruise.plugins.CruiseDatabase.utils.cruConnection;
import com.cruise.plugins.CruiseDatabase.utils.cruConnectionObject;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Hello world!
 *
 */
public class CruiseDatabase implements PluginInterface
{

	PlugInMetaData pmd = null;
    public CruiseDatabase() {
    	new PlugInMetaData("CruiseDatabase","0.0.1","SJC","Database access plugin");
    	pmd.getActions().add(new Action("CruiseTest", "Test API Call"));
		pmd.getActions().get(0).getActionParams().add(new ActionParameter("SampleParam","false","Sample Value","Dummy Parameter for test"));
		pmd.getActions().get(0).getActionParams().add(new ActionParameter("SampleParam2","false","Sample Value2","Dummy Parameter 2 for test"));
		
    	pmd.getActions().add(new Action("cDBCreatePool", "Create a new PooledConnection"));
		pmd.getActions().get(0).getActionParams().add(new ActionParameter("PoolName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(0).getActionParams().add(new ActionParameter("DriverClassName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(0).getActionParams().add(new ActionParameter("jdbcUrl","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(0).getActionParams().add(new ActionParameter("username","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(0).getActionParams().add(new ActionParameter("password","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(0).getActionParams().add(new ActionParameter("maximumPoolSize","false","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(0).getActionParams().add(new ActionParameter("minimumIdle","false","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		
    
    
    }

	public boolean executePlugin(SessionObject so, Services service) {
		try {
			System.out.println(so.getCruiseMapper().writerWithDefaultPrettyPrinter().writeValueAsString(pmd));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		boolean ret = false;
		String action = service.getAction();
		//{"cDBAdd","cDBUpdate","cDBDelete","cDBFind","cDBCreateConnection", "cDBGetConnection"};
		switch (action) {
		case "cDBCreatePool":
			ret = cruConnection.createConnectionPool(so,service);
			break;
		case "cDBGetConnection":
			cruConnectionObject cruConn = null;
			try {
				cruConn = cruConnection.getConnection(so, service);
				if(null != cruConn) {
					so.setRequestState(service.getParameter("PoolName").getValue(), cruConn);
				}else {
					Clog.Error(so, "ser", "90010", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "90020", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}
			break;
		default:
			Clog.Error(so, "service", "90001", "Invalid Action supplied:"+action);
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
