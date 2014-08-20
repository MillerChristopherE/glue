package org.glue.modules.gcloud;

import org.apache.log4j.Logger
import org.glue.unit.om.GlueContext
import org.glue.unit.om.GlueModule
import org.glue.unit.om.GlueProcess
import org.glue.unit.om.GlueUnit
import org.glue.unit.process.DefaultJavaProcessProvider
import org.glue.unit.process.JavaProcess
import org.glue.unit.exceptions.*;


/**
 * Interface with bq.
 */
public class BqModule implements GlueModule {
	
	private static final Logger LOG = Logger.getLogger(BqModule)
	
	GlueContext ctx
	
    String binPath = "bq";
	Map<String, ConfigObject> connections=[:];
    
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
        binPath = config.binPath ?: binPath;
		config.connections.each { String key, ConfigObject c ->
			print "loading connection $key"
			connections[key] = c
		}
        
	}
    
    @Override
    public Map getInfo() {
        return [
            'connections': this.connections,
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
    Map getConnectionConfig(GlueContext context, String connection)
    {
        if(!connections[connection])
        {
            throw new ModuleConfigurationException("No such bq connection named $connection");
        }
        return connections[connection];
    }
    
    void appendOptions(GlueContext context, String connection, Map options, List<String> append, boolean isCommon=false)
    {
        options.each { k1, v ->
            String k = k1.toString();
            assert k.length() > 0, "Invalid empty key"
            if(v == false)
            {
                assert k.length() > 1, "Must use long name when setting option to false (key.length is 1 for $k)"
                append << "--no$k"
            }
            else
            {
                if(isCommon != commonSwitches.contains(k))
                {
                    return;
                }
                
                if(k.length() == 1)
                {
                    append << "-$k"
                }
                else
                {
                    append << "--$k"
                }
                if(v == true)
                {
                    // Switch is present to enable.
                }
                else
                {
                    append << v.toString()
                }
            }
        }
    }
    
    void appendConnectionCommonOptions(GlueContext context, String connection, List<String> append)
    {
        def commonOptions = getConnectionConfig(context, connection).commonOptions;
        boolean wantHeadless = true;
        boolean wantQuiet = true;
        if(commonOptions)
        {
            appendOptions(context, connection, commonOptions, append, true);
            wantHeadless = !commonOptions.containsKey("headless");
            wantQuiet = !commonOptions.containsKey("quiet") && !commonOptions.containsKey("q");
        }
        if(wantHeadless)
        {
            append << "--headless";
        }
        if(wantQuiet)
        {
            append << "--quiet";
        }
    }
    
    static class BqRuntimeException extends RuntimeException
    {
        public BqRuntimeException(String msg)
        {
            super(msg);
        }
    }
    
    String getProcessIO(GlueContext context, Process proc, Closure created, boolean throwOnStderr=true, boolean throwOnErrorCode=true)
    {
        def sbout = new StringBuffer();
        def sberr = new StringBuffer();
        //proc.waitForProcessOutput(sbout, sberr);
        proc.consumeProcessOutput(sbout, sberr);
        if(created)
        {
            created(proc);
        }
        proc.waitFor();
        boolean showerrorcode = throwOnErrorCode && proc.exitValue() != 0;
        if(throwOnStderr && sberr.length() > 0 && !showerrorcode)
        {
            throw new BqRuntimeException("Command `" + args + "` failed: " + sberr.toString());
        }
        if(showerrorcode)
        {
            String extra = "";
            if(sberr.length() > 0)
            {
                extra = ": " + sberr.toString();
            }
            throw new BqRuntimeException("Command `" + args + "` returned error exit value " + proc.exitValue() + extra);
        }
        return sbout.toString();
    }
    
    String getProcessOutput(GlueContext context, Process proc, boolean throwOnStderr=true, boolean throwOnErrorCode=true)
    {
        return getProcessIO(context, proc, null, throwOnStderr, throwOnErrorCode);
    }
    
    Process startProcess(GlueContext context, String connection, List<String> args)
    {
        def c = getConnectionConfig(context, connection);
        
        def cmd = [];
        cmd << binPath;
        appendConnectionCommonOptions(context, connection, args);
        for(String arg : args)
        {
            cmd << arg;
        }
        
        String workingDir = ".";
        if(true)
        {
            File logFile = context?.logger?.getLogFile()
            if(logFile)
            {
                File parent = logFile.getParentFile();
                if(parent)
                {
                    workingDir = parent.getAbsolutePath();
                }
            }
        }
        
        println "BqModule.startProcess: connection=$connection, command=$cmd";
        
        Process proc = cmd.execute(null, new File(workingDir));
        return proc;
    }
    
    ///
    String run(GlueContext context, String connection, List<String> args, boolean throwOnStderr=true, boolean throwOnErrorCode=true)
    {
        return getProcessOutput(context, startProcess(context, connection, args), throwOnStderr, throwOnErrorCode);
    }
    
    ///
    String cp(GlueContext context, String connection, String old_table, String new_table, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "cp";
        appendOptions(context, connection, options, args);
        args << old_table;
        args << new_table;
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    ///
    String extract(GlueContext context, String connection, String source_table, List<String> destination_uris, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "extract";
        appendOptions(context, connection, options, args);
        args << source_table;
        args << destination_uris;
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    /// Shortcut for one destination URI.
    String extract(GlueContext context, String connection, String source_table, String destination_uri, Map<String, Object> options)
    {
        return extract(context, connection, source_table, [ destination_uri ] as List<String>, options);
    }
    
    /// Location is likely dataset.table
    String head(GlueContext context, String connection, String location, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "head";
        appendOptions(context, connection, options, args);
        args << location;
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    ///
    String insert(GlueContext context, String connection, String table, String filePath, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "insert";
        appendOptions(context, connection, options, args);
        args << table;
        File f = new File(filePath);
        if(!f.isFile())
        {
            throw new IllegalArgumentException("Not a file: $filePath");
        }
        return getProcessIO(context, startProcess(context, connection, args),
            { proc ->
                f.eachLine { line ->
                    proc << line;
                }
            },
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    ///
    String load(GlueContext context, String connection, String destination, String source, String schema, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "load";
        appendOptions(context, connection, options, args);
        args << destination;
        args << source;
        if(schema)
        {
            args << schema;
        }
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    /// No schema needed.
    String load(GlueContext context, String connection, String destination, String source, Map<String, Object> options)
    {
        return load(context, connection, destination, source, null, options);
    }
    
    ///
    String ls(GlueContext context, String connection, String location, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "load";
        appendOptions(context, connection, options, args);
        if(location)
        {
            args << location;
        }
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    // Shortcut for no list location.
    String ls(GlueContext context, String connection, Map<String, Object> options)
    {
        return ls(context, connection, null, options);
    }
    
    ///
    String mk(GlueContext context, String connection, String location, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "mk";
        appendOptions(context, connection, options, args);
        args << location;
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    /// Shortcut to create a table.
    String mkTable(GlueContext context, String connection, String table, String schema, Map<String, Object> options)
    {
        if(true)
        {
            options = options.clone();
            options.table = true;
        }
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "mk";
        appendOptions(context, connection, options, args);
        args << table;
        if(schema)
        {
            args << schema;
        }
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    /// Shortcut to create a table.
    String mkTable(GlueContext context, String connection, String table, Map<String, Object> options)
    {
        return mkTable(context, connection, table, null, options);
    }
    
    ///
    String query(GlueContext context, String connection, String query, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "query";
        appendOptions(context, connection, options, args);
        args << query;
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    ///
    String rm(GlueContext context, String connection, String location, boolean rf, Map<String, Object> options)
    {
        if(rf)
        {
            options = options.clone();
            options.r = true;
            options.f = true;
        }
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "rm";
        appendOptions(context, connection, options, args);
        if(location)
        {
            args << location;
        }
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    ///
    String rm(GlueContext context, String connection, String location, Map<String, Object> options)
    {
        return rm(context, connection, location, false, options);
    }
    
    ///
    String show(GlueContext context, String connection, String location, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "show";
        appendOptions(context, connection, options, args);
        if(location)
        {
            args << location;
        }
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    /// Shortcut for no show location.
    String show(GlueContext context, String connection, Map<String, Object> options)
    {
        return show(context, connection, null, options);
    }
    
    ///
    String update(GlueContext context, String connection, String location, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "update";
        appendOptions(context, connection, options, args);
        args << location;
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    /// Shortcut to update table.
    String updateTable(GlueContext context, String connection, String table, String schema, Map<String, Object> options)
    {
        if(true)
        {
            options = options.clone();
            options.table = true;
        }
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "update";
        appendOptions(context, connection, options, args);
        args << table;
        if(schema)
        {
            args << schema;
        }
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    /// Shortcut to update table.
    String updateTable(GlueContext context, String connection, String table, Map<String, Object> options)
    {
        return updateTable(context, connection, table, null, options);
    }
    
    ///
    String wait(GlueContext context, String connection, String job_id, int seconds, Map<String, Object> options)
    {
        List<String> args = new ArrayList<String>();
        appendOptions(context, connection, options, args, true);
        args << "wait";
        appendOptions(context, connection, options, args);
        if(job_id)
        {
            args << job_id;
        }
        if(seconds >= 0)
        {
            args << "$seconds";
        }
        return run(context, connection, args,
            options.throwOnStderr == false ? false : true,
            options.throwOnErrorCode == false ? false : true);
    }
    
    ///
    String wait(GlueContext context, String connection, String job_id, Map<String, Object> options)
    {
        return wait(context, connection, job_id, -1, options);
    }
    
    
    Set<String> commonSwitches = [
        "apilog",
        "api",
        "api_version",
        "debug_mode",
        "trace",
        "bigqueryrc",
        "credential_file",
        "discovery_file",
        "synchronous_mode",
        "project_id",
        "dataset_id",
        "job_id",
        "fingerprint_job_id",
        "quiet",
        "headless",
        "format",
        "job_property",
        "use_gce_service_account",
        "service_account",
        "service_account_private_key_file",
        "service_account_private_key_password",
        "service_account_credential_file",
        "max_rows_per_request",
        ] as HashSet<String>;
    

}
