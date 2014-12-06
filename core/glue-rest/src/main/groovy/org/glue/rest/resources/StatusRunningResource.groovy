package org.glue.rest.resources;

import org.apache.log4j.Logger
import org.codehaus.jackson.map.ObjectMapper
import org.restlet.data.MediaType
import org.restlet.data.Status
import org.restlet.representation.Representation
import org.restlet.representation.StringRepresentation
import org.restlet.resource.Get
import org.restlet.resource.ServerResource
import org.glue.unit.exec.GlueExecutor
import org.glue.unit.exec.UnitExecutor
import org.glue.unit.om.GlueUnitMultiQueue

import java.util.Map

import org.glue.unit.exec.WorkflowsStatus

/**
 * Reads the status of already running workflows
 */
class StatusRunningResource extends ServerResource {

	private static final Logger LOG = Logger.getLogger(UnitStatusResource.class)
	static final ObjectMapper mapper = new ObjectMapper()


	WorkflowsStatus executor;
	

	public StatusRunningResource(
			WorkflowsStatus executor) {
		super();
		this.executor = executor
	}

	@Get("json")
	public Representation represent(Representation entity) {

		Representation rep
		try{

			
			def qwfs = executor.queuedWorkflows()
			
			def runningStats = executor.runningWorkflows().collect { ctx ->
                def x = [unitId:ctx.unitId, name:ctx.unit.name,
					queued: qwfs.contains(ctx.unit.name)
				]
                if(ctx.unit instanceof GlueUnitMultiQueue){
                    x.queue = ((GlueUnitMultiQueue)ctx.unit).queue
                }
                return x
		    } 
			
			setStatus(Status.SUCCESS_CREATED);
			
			rep = new StringRepresentation(this.mapper.writeValueAsString(
				[out:runningStats]
				),
					MediaType.APPLICATION_JSON);
		}catch(Throwable t){
		
			LOG.error(t.toString(), t)
			def out=[:]
			out.error=t
			setStatus Status.SERVER_ERROR_INTERNAL, t, t.toString()

			rep = new StringRepresentation( mapper.writeValueAsString(out) )
		}

		return rep;
	}
}
