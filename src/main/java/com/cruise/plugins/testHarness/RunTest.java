package com.cruise.plugins.testHarness;
import com.corecruise.cruise.testharness.ValidateUser;
import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.SessionObjectJSON;
import com.corecruise.cruise.services.interfaces.PluginClientInterface;
import com.corecruise.cruise.services.interfaces.PluginInterface;
import com.corecruise.cruise.services.utils.Services;


public class RunTest implements PluginClientInterface {
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
						"		    \"services\" : ["+
						/*"		      \"SampleService\" : {"+
						"		        \"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"GetPlugIn_Info\","+
						"		          \"action\" : \"info\""+
						"		        }"+
						"		      },"+*/
						"		        {\"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"AddConnection\","+
						"		          \"action\" : \"cDBCreatePool\","+
						"		          \"poolName\" : \"MyPool\","+
						"		          \"DriverClassName\" : \"org.mariadb.jdbc.Driver\","+
						"		          \"maximumPoolSize\" : \"25\","+
						"		          \"minimumIdle\" : \"5\","+
						"		          \"jdbcUrl\" : \"jdbc:mysql://localhost:3306/cruisecore?useSSL=false\","+
						"		          \"schema\" : \"cruisecore\","+
						"		          \"username\" : \"root\","+
						"		          \"password\" : \"admin\""+
						"		        }"+
						"		      },"+
						"		      {\"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"UpdateRecord\","+
						"		          \"action\" : \"update\","+
						"		          \"poolName\" : \"MyPool\","+
						"		          \"COMPONENTINDEX\" : \"0000\","+
						"		          \"COMPONENTNAME\" : \"Thank you disillusionment\","+
						"		          \"tableName\" : \"cru_components\""+
						"		        }"+
						"		      },"+
						"		      {\"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"selectAll\","+
						"		          \"action\" : \"select\","+
						"		          \"poolName\" : \"MyPool\","+
						"		          \"tableName\" : \"cru_components\""+
						"		        }"+
						"		      },"+
						"		      {\"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"action\" : \"echo\""+
						"		        }"+
						"		      }"+
						/*","+
						"		      { \"parameters\" : {"+
						"		          \"pluginName\" : \"CruiseDatabase\","+
						"		          \"service\" : \"Pool Information\","+
						"		          \"action\" : \"cDBGetPoolInfo\","+
						"		          \"PoolName\" : \"MyPool\""+
						"		        }"+
						"		      }"+*/

						"		    ]"+
						"		  }"+
						"		}";
		
		
		SessionObjectJSON sowp1 = new SessionObjectJSON();
		sowp1.go(this, vu , sample ,true);
		try {
			System.out.println(sowp1.getResponseJSONPP());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		
	}
	@Override
	public boolean PreProcess(SessionObject cruise, Services service) {
		// TODO Auto-generated method stub
		return true;
	}
	@Override
	public boolean PostProcess(SessionObject cruise, Services service) {
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

