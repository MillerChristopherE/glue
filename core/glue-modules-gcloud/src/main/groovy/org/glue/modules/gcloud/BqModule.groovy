package org.glue.modules.gcloud;

import org.apache.log4j.Logger
import org.glue.unit.om.GlueContext
import org.glue.unit.om.GlueModule
import org.glue.unit.om.GlueProcess
import org.glue.unit.om.GlueUnit
import org.glue.unit.process.DefaultJavaProcessProvider
import org.glue.unit.process.JavaProcess
import org.glue.unit.exceptions.*;

import com.google.api.client.googleapis.auth.oauth2.*;
import com.google.api.services.bigquery.*;
import com.google.api.services.bigquery.model.*;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.*;
import com.google.api.client.json.*;
import com.google.api.client.json.jackson.*;
import com.google.api.client.auth.oauth2.*;
import com.google.api.client.extensions.appengine.http.*;
import com.google.api.client.extensions.appengine.auth.oauth2.*;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.*;


/**
 * Interface with bq.
 */
public class BqModule implements GlueModule {
	
	private static final Logger LOG = Logger.getLogger(BqModule)
	
	GlueContext ctx
	
	Map<String, ConfigObject> connections=[:];
	//String defaultConnection = null;
	
    Map<String, Bigquery> cachedServices=[:];
    
	void destroy(){
	}

	@Override
	public Boolean canProcessRun(GlueProcess process, GlueContext context) {
		return true;
	}

	@Override
	public void configure(String unitId, ConfigObject config) {
		
	}

	@Override
	public String getName() {
		return "bq";
	}

	@Override
	public void init(ConfigObject config) {
		if(!config.connections) {
			new ModuleConfigurationException("Can't find any connections in config!")
		}
		config.connections.each { String key, ConfigObject c ->
			print "loading connection $key"
            
            /*
			if(c.isDefault) {
				defaultConnection=key;
			}
			//set the default connections to the first key if not set
			//if c.isDefault is specified this will be overwritten above
			if(!defaultConnection)
				defaultConnection = key;
            */
            
            if(!c.account){
                throw new ModuleConfigurationException("The account for $key was not set")
            }
			
            if(!c.keyFile || !new File(c.keyFile.toString()).exists()){
                throw new ModuleConfigurationException("The keyFile for $key was not found")
            }
            
            if(!c.project){
                throw new ModuleConfigurationException("The project for $key was not set")
            }

			connections[key]=c
		}
        
	}
    
    @Override
    public Map getInfo() {
        return [
            'connections': this.connections,
            //'defaultConnection': this.defaultConnection,
        ]
    }

	@Override
	public void onProcessKill(GlueProcess process, GlueContext context){
	}

	@Override
	public void onProcessFail(GlueProcess process, GlueContext context,
	Throwable t) {
	}

	@Override
	public void onProcessFinish(GlueProcess process, GlueContext context) {
	}

	@Override
	public void onProcessStart(GlueProcess process, GlueContext context) {
	}

	@Override
	public void onUnitFail(GlueUnit unit, GlueContext context) {
	}

	@Override
	public void onUnitFinish(GlueUnit unit, GlueContext context) {
	}

	@Override
	public void onUnitStart(GlueUnit unit, GlueContext context) {
		ctx = context
	}

    // Module user functions:
    
    /// Get glue config for the connection.
    ConfigObject getConnectionConfig(GlueContext context, String connection)
    {
        if(!connections[connection])
        {
            throw new ModuleConfigurationException("No such bq connection named $connection")
        }
        return connections[connection];
    }

    /// Get a service builder.
    Bigquery.Builder getServiceBuilder(GlueContext context, String connection)
    {
        ConfigObject c = getConnectionConfig(contect, connection);
        
        HttpTransport httpTransport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();

        File keyFile = new File(c.keyFile.toString());

        GoogleCredential.Builder credBuilder = new GoogleCredential.Builder();
        credBuilder.setJsonFactory(jsonFactory);
        credBuilder.setTransport(httpTransport);
        credBuilder.setServiceAccountId(c.account.toString());
        credBuilder.setServiceAccountPrivateKeyFromP12File(keyFile);
        credBuilder.setServiceAccountScopes(Collections.singleton(BigqueryScopes.BIGQUERY));

        GoogleCredential credentials = credBuilder.build();

        return new Bigquery.Builder(httpTransport, jsonFactory, credentials);
            
    }
    
    /// Build a new service. Consider using getService to use a cached service. This method does not set the cached service.
    Bigquery buildService(GlueContext context, String connection)
    {
        Bigquery service = getServiceBuilder(context, connection);
        if(service == null || service.jobs() == null)
        {
            throw new NullPointerException("Service is null (bq)");
        }
        return service;
    }
    
    /// Allows specifying if the cached service should be replaced.
    Bigquery getService(GlueContext context, String connection, boolean getNewCache)
    {
        Bigquery service = cachedServices[connection];
        if(service)
        {
            if(getNewCache)
            {
                //service.close();
                service = null;
                cachedServices[connection] = null;
            }
            else
            {
                return service;
            }
        }
        service = buildService(context, connection);
        cachedServices[connection] = service;
        return service;
    }
    
    /// Gets the cached service, or creates one if not created yet.
    Bigquery getService(GlueContext context, String connection)
    {
        return getService(context, connection, false);
    }
    
    ///
    Dataset referenceDataset(GlueContext context, String connection, String dataset)
    {
        ConfigObject c = getConnectionConfig(contect, connection);
        
        DatasetReference datasetRef = new DatasetReference();
        datasetRef.setProjectId(c.project.toString());
        datasetRef.setDatasetId(dataset);
        
        Dataset ds = new Dataset();
        ds.setDatasetReference(datasetRef);
        
        return ds;
    }
    
    ///
    Bigquery.Datasets.Insert getDatasetInsertRequest(GlueContext context, String connection, Dataset dataset)
    {
        ConfigObject c = getConnectionConfig(contect, connection);
        return getService(context, connection).datasets().insert(c.project.toString(), dataset);
    }
    
    ///
    Bigquery.Datasets.Insert getDatasetInsertRequest(GlueContext context, String connection, String dataset)
    {
        return getDatasetInsertRequest(context, connection, referenceDataset(context, connection, dataset));
    }
    
    ///
    Dataset insertDataset(GlueContext context, String connection, Dataset dataset)
    {
        return getDatasetInsertRequest(context, connection, dataset).execute();
    }
    
    ///
    Dataset insertDataset(GlueContext context, String connection, String dataset)
    {
        return getDatasetInsertRequest(context, connection, dataset).execute();
    }
    
    ///
    Job getLoadJob(GlueContext context, String connection, String dataset, String destTable, String schema, Map options)
    {
        ConfigObject c = getConnectionConfig(contect, connection);
        
        TableReference destinationTable = new TableReference();
        destinationTable.setProjectId(c.project.toString());
        destinationTable.setDatasetId(dataset);
        destinationTable.setTableId(destTable);
        
        JobConfigurationLoad jobLoad = new JobConfigurationLoad();
        jobLoad.setSchema(getTableSchema(schema));
        jobLoad.setSourceFormat(options.format ?: "CSV");
        jobLoad.setDestinationTable(destinationTable);
        jobLoad.setCreateDisposition(options.create == true ? "CREATE_IF_NEEDED" : "CREATE_NEVER");
        jobLoad.setWriteDisposition(options.replace == true ? "WRITE_TRUNCATE" :
            (options.append == true ? "WRITE_APPEND" : "WRITE_EMPTY")
            );
        
        JobConfiguration jobConfig = new JobConfiguration();
        jobConfig.setLoad(jobLoad);

        JobReference jobRef = new JobReference();
        jobRef.setProjectId(c.project.toString());
        if(options.jobId)
        {
            jobRef.setJobId(options.jobId);
        }

        Job job = new Job();
        job.setConfiguration(jobConfig);
        job.setJobReference(jobRef);
        
        return job;
    }
    
    /// Does not honor options.content_type because the contents object specifies the type.
    Bigquery.Jobs.Insert getJobLoadInsertRequest(GlueContext context, String connection, String dataset, String destTable,
        String schema, Map options, AbstractInputStreamContent contents
        )
    {
        ConfigObject c = getConnectionConfig(contect, connection);
        
        return service.jobs().insert(c.project.toString(),
            getLoadJob(context, connection, dataset, destTable, schema, options),
            contents)
    }
    
    /// Honors options.content_type.
    Bigquery.Jobs.Insert getJobLoadInsertRequest(GlueContext context, String connection, String dataset, String destTable,
        String schema, Map options, File srcFile
        )
    {
        FileContent contents = new FileContent(options.content_type ?: DEFAULT_CONTENT_TYPE, srcFile);
        return getJobLoadInsertRequest(context, connection, dataset, destTable, schema, options, contents);
    }
    
    /// Honors options.content_type.
    Bigquery.Jobs.Insert getJobLoadInsertRequest(GlueContext context, String connection, String dataset, String destTable,
        String schema, Map options, byte[] data
        )
    {
        ByteArrayContent contents = new ByteArrayContent(options.content_type ?: DEFAULT_CONTENT_TYPE, data);
        return getJobLoadInsertRequest(context, connection, dataset, destTable, schema, options, contents);
    }
    
    /// Insert data from local file, waits for job execution.
    void insert(GlueContext context, String connection, String dataset, String destTable,
        String schema, Map options, String srcFileName)
    {
        Job job = getJobLoadInsertRequest(context, connection, dataset, destTable, schema, options, new File(srcFileName)).execute();
        if(job == null)
        {
            throw new NullPointerException("Job is null");
        }
        
        while(true)
        {
            String status = job.getStatus().getState();

            if (status != null || ("DONE").equalsIgnoreCase(status))
            {
                ErrorProto errorResult = job.getStatus().getErrorResult();
                if(errorResult != null)
                {
                    throw new RuntimeException("Error running job: " + errorResult);
                }
                break;
            }

            Thread.sleep(1000);
        }
    }
    
    // ---
    
    protected TableSchema getTableSchema(String schema)
    {
        List<TableFieldSchema> schemaList = new ArrayList<TableFieldSchema>();
        
        for(String field : schema.split("\\s*,\\s*"))
        {
            String[] x = field.split("\\s*:\\s*");
            /*if(x.length == 1)
            {
                schemaList.add(new TableFieldSchema().setName(x[0]));
            }
            else*/ if(x.length == 2)
            {
                schemaList.add(new TableFieldSchema().setName(x[0]).setType(x[1]));
            }
            else
            {
                throw new RuntimeException("Invalid schema: $schema");
            }
        }
        
        TableSchema ts = new TableSchema();
        ts.setFields(schemaList);
        return ts;
    }
    
    protected final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    

}
