package org.glue.unit.exec.impl.queue

import groovy.util.ConfigObject

import java.net.URL
import java.util.List;
import java.util.Map
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.log4j.Logger
import org.glue.unit.exceptions.UnitSubmissionException
import org.glue.unit.exec.GlueExecutor
import org.glue.unit.exec.GlueState
import org.glue.unit.exec.UnitExecutor
import org.glue.unit.exec.impl.WorkflowRunner
import org.glue.unit.log.GlueExecLoggerProvider
import org.glue.unit.om.GlueContext
import org.glue.unit.om.GlueContextBuilder
import org.glue.unit.om.GlueUnit
import org.glue.unit.om.GlueUnitMultiQueue
import org.glue.unit.om.GlueUnitBuilder
import org.glue.unit.process.DefaultJavaProcessProvider
import org.glue.unit.repo.GlueUnitRepository
import org.glue.unit.status.UnitStatus
import org.glue.unit.exec.WorkflowsStatus

/**
 * 
 * Implements the GlueExecutor and provides one or more queues to in which workflows will be executed.
 *
 */
@Typed
class QueuedExecServiceMultiQueueImpl implements GlueExecutor, WorkflowsStatus{


	static final Logger log = Logger.getLogger(QueuedExecServiceMultiQueueImpl.class)

    protected GlueUnitRepository glueUnitRepository

    protected GlueUnitBuilder glueUnitBuilder
    
	/**
	 * key = queue name
	 */
    protected Map<String, QueuedExecServiceImpl> queues = new HashMap<String, QueuedExecServiceImpl>()
	
    public static class QueueInfo{
        public String name;
        public int maxRunningWorkflows;
    }
    
	/**
	 * 
	 * @param maxGlueProcesses the max number of glue processes allowed to run at any time
	 * @param glueUnitRepository
	 * @param glueUnitBuilder
	 */
	@Typed(TypePolicy.MIXED)
	public QueuedExecServiceMultiQueueImpl(List<QueueInfo> qinfos, Map<String, String> config, Collection<String> javaOpts, Collection<String> classPath, GlueUnitRepository glueUnitRepository,
	GlueUnitBuilder glueUnitBuilder,
    String execConf,
	String moduleConf,
	GlueExecLoggerProvider logProvider = null, GlueContextBuilder contextBuilder = null) {
    
		this.glueUnitRepository = glueUnitRepository
		this.glueUnitBuilder = glueUnitBuilder
        
        qinfos.each{ qinfo ->
            log.info("Adding queue $qinfo.name with $qinfo.maxRunningWorkflows max running workflows")
            assert !queues[qinfo.name], "Duplicate queue found"
            queues[qinfo.name] = new QueuedExecServiceImpl(
                qinfo.maxRunningWorkflows,
                config,
                javaOpts,
                classPath,
                glueUnitRepository,
                glueUnitBuilder,
                execConf,
                moduleConf,
                logProvider,
                contextBuilder
            )
        }
        log.info("Setup ${qinfos.size()} queues")
        if(!queues["default"]){
            throw new RuntimeException("Did not find default queue")
        }

	}
	
	void terminate(String unitId){
        queues.each { k, v -> v.terminate(unitId) }
	}


	List<GlueContext> runningWorkflows(){
        ArrayList<GlueContext> result = new ArrayList<GlueContext>();
        queues.each { k, v -> result.addAll(v.runningWorkflows()) }
        return result
	}
	
	Set<String> queuedWorkflows(){
        Set<String> result = new HashSet<String>();
        queues.each { k, v -> result.addAll(v.queuedWorkflows()) }
        return result
	}
	
	
	GlueState getStatus(String unitId){
        def result = queues.findResult{String k, QueuedExecServiceImpl v ->
            GlueState x = v.getStatus(unitId)
            return x == GlueState.FINISHED ? null : x;
        }
        return result ?: GlueState.FINISHED
	}
	
	public Map<String, UnitExecutor> getUnitList() { [:] }

	/**
	 * Returns the glue context
	 * @param unitId
	 * @return
	 */
	GlueContext getContext(String unitId){
		return queues.findResult { String k, QueuedExecServiceImpl v -> v.getContext(unitId) }
	}


	double getProgress(String unitId){
        return queues.inject(1.0D, { double acc, String k, QueuedExecServiceImpl v ->
            return Math.min(acc, v.getProgress(unitId))
        })
	}

	void waitFor(String unitId){
        queues.each { k, v -> v.waitFor(unitId) }
	}

	void waitFor(String unitId, long time, java.util.concurrent.TimeUnit timeUnit){
        queues.each { k, v -> v.waitFor(unitId, time, timeUnit) }
	}

	/**
	 * Notifies the execution threads to complete and shutdown.
	 * This method does not wait for shutdown and returns.
	 */
	public void shutdown(){
		queues.each { k, v -> v.shutdown() }
	}

	/**
	 * Start shutdown procedure and wait for all GlueUnits to complete processing
	 */
	public void waitUntillShutdown(){
        queues.each { k, v -> v.waitUntillShutdown() }
	}

	/**
	 * Submits the GlueUnit based on its logical name, e.g. the file name used to write the glue unit.<br/>
	 *
	 */
	@Override
	public String submitUnitAsName(String unitName,Map<String,Object> params, String unitId = null)
	throws UnitSubmissionException {

		if(glueUnitRepository == null){
			throw new UnitSubmissionException("No repository was defined for this GlueExecutor therefore GlueUnit(s) cannot be submit by name ")
		}

		//query the GlueUnitRepository for the unit
		GlueUnit unit = glueUnitRepository.find( unitName )

		if(!unit) {
			throw new UnitSubmissionException("Could not find $unitName in repository " + glueUnitRepository)
		}


		return this.submit(unit, params, unitId)
	}

	/**
	 * Helper method that submits the GlueUnit as a ConfigObject.<br/>
	 * Note: This will not retrieve the unit from the UnitRepository.
	 */
	@Override
	public String submitUnitAsConfig(ConfigObject config,Map<String,Object> params)
	throws UnitSubmissionException {
		GlueUnit unit = glueUnitBuilder.build(config)
		submit(unit, params)
	}


	/**
	 * Helper method that submits the GlueUnit as a text String.<br/>
	 * Note: This will not retrieve the unit from the UnitRepository.
	 */
	@Override
	public String submitUnitAsText(String unitText,Map<String,Object> params)
	throws UnitSubmissionException {
		GlueUnit unit = glueUnitBuilder.build(unitText)
		submit(unit, params)
	}

	/**
	 * Helper method that submits the GlueUnit from a URL.<br/>
	 * Note: This will not retrieve the unit from the UnitRepository.
	 */
	@Override
	public String submitUnitAsUrl(URL unitUrl, Map<String,Object> params) throws UnitSubmissionException {
		GlueUnit unit = glueUnitBuilder.build(unitUrl)
		submit(unit, params)
	}


	/**
	 * See QueuedExecServiceImpl.submit
	 */
	protected synchronized String submit(GlueUnit unit,Map<String,Object> params, String unitId = null)
	throws UnitSubmissionException {
        String queueName = "default";
        if(unit instanceof GlueUnitMultiQueue){
            queueName = ((GlueUnitMultiQueue)unit).queue
        }
        QueuedExecServiceImpl queue = queues[queueName];
        if(!queue){
            println "Workflow ${unit.name} cannot run, no such queue $queueName"
            return "0"
        }
        log.info("Submitting workflow ${unit.name} to queue $queueName")
        return queue.submit(unit, params, unitId)
	}
    
}