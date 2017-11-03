package com.cruise.plugins.CruiseDatabase.testHarness;

import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.SessionObjectWebJSON;
import com.corecruise.cruise.SessionObjectWebParams;
import com.corecruise.cruise.services.interfaces.CruiseInterface;
import com.corecruise.cruise.services.interfaces.PluginInterface;
import com.corecruise.cruise.services.utils.Application;
import com.corecruise.cruise.services.utils.CollectionBean;
import com.cruise.plugins.Action;
import com.cruise.plugins.ActionParameter;
import com.cruise.plugins.PlugInMetaData;
import com.fasterxml.jackson.databind.JsonNode;

public class RunTest implements CruiseInterface {
	public RunTest() {
		startApp();
	}
	public void startApp() {
        
		ValidateUser vu = new ValidateUser();
		vu.initializeValidation();
		
		String sample = 
				"{"+
						"		  \"Application\" : {"+
						"		    \"parameters\" : {"+
						"		      \"name\" : \"sampleapp\","+
						"		      \"id\" : \"sampleid\""+
						"		    },"+
						"		    \"credentials\" : {"+
						"		      \"parameters\" : {"+
						"		        \"password\" : \"admin\","+
						"		        \"username\" : \"admin\""+
						"		      }"+
						"		    },"+
						"		    \"services\" : {"+
						/*"		      \"SampleService\" : {"+
						"		        \"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"GetPlugIn_Info\","+
						"		          \"action\" : \"info\""+
						"		        }"+
						"		      },"+*/
						"		      \"DatabaseConnection\" : {"+
						"		        \"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"AddConnection\","+
						"		          \"action\" : \"cDBCreatePool\","+
						"		          \"PoolName\" : \"MyPool\","+
						"		          \"DriverClassName\" : \"org.mariadb.jdbc.Driver\","+
						"		          \"maximumPoolSize\" : \"25\","+
						"		          \"minimumIdle\" : \"5\","+
						"		          \"jdbcUrl\" : \"jdbc:mysql://localhost:3306/cruisecore?useSSL=false\","+
						"		          \"schema\" : \"cruisecore\","+
						"		          \"username\" : \"root\","+
						"		          \"password\" : \"admin\""+
						"		        }"+
						"		      },"+
						
						"		      \"TableUpdate\" : {"+
						"		        \"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"UpdateRecord\","+
						"		          \"action\" : \"update\","+
						"		          \"PoolName\" : \"MyPool\","+
						"		          \"fromlist\" : \"cru_components\","+
						"		          \"COMPONENTINDEX\" : \"0000\","+
						"		          \"COMPONENTNAME\" : \"Thank you disillusionment\","+
						"		          \"TableName\" : \"cru_components\""+
						"		        }"+
						"		      },"+
						
						"		      \"TableSelect\" : {"+
						"		        \"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"selectAll\","+
						"		          \"action\" : \"select\","+
						"		          \"PoolName\" : \"MyPool\","+
						"		          \"fromlist\" : \"cru_components\","+
						"		          \"TableName\" : \"cru_components\""+
						"		        }"+
						"		      }"+
						/*","+
						"		      \"DatabaseInformation\" : {"+
						"		        \"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"Pool Information\","+
						"		          \"action\" : \"cDBGetPoolInfo\","+
						"		          \"PoolName\" : \"MyPool\""+
						"		        }"+
						"		      }"+*/

						"		    }"+
						"		  }"+
						"		}";
		
		
		SessionObjectWebJSON sowp1 = new SessionObjectWebJSON();
		sowp1.go(this, vu , sample ,true);
		try {
			System.out.println(sowp1.getResponseJSONPP());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		
	}
	@Override
	public boolean PreProcess(SessionObject cruise, PluginInterface plugIn) {
		// TODO Auto-generated method stub
		return true;
	}
	@Override
	public boolean PostProcess(SessionObject cruise, PluginInterface plugIn) {
		// TODO Auto-generated method stub
		return true;
	}
	@Override
	public void ProcessingError(SessionObject cruise) {
		System.out.println("***************ERROR*******************");
		System.out.println("***************ERROR*******************");
		System.out.println("***************ERROR*******************");
		try {
			System.out.println(cruise.getResponseJSONPP());
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("***************ERROR*******************");
		System.out.println("***************ERROR*******************");
		System.out.println("***************ERROR*******************");
		
	}

}

