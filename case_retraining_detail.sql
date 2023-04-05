SELECT mm.id, 
mm.subject, 
mm.status, 
mm.created_on ingested_date, 
mm.reference_id ingestion_ref_id, 
ins.business_key case_ref_id, 
ins.id process_instance_id, 
ins.created_on case_created_on, 
task.name task_name, 
task.created_on task_created_on, 
task.status task_status, 
var.reason,
var.category routed_category,
op.json_key retraining_key,
TRIM(BOTH '"' FROM op.json_value) retraining_value
FROM ingestion.mailmessage mm
LEFT JOIN (
	select id, business_key, created_on from bpmengine.run_proc_inst
	union
	select id, business_key, created_on from bpmengine.historical_proc_inst
) ins ON ins.business_key = mm.reference_id
LEFT JOIN (
	select id, proc_inst_id, name, created_on, status from bpmengine.run_task
	union
	select id, proc_inst_id, name, created_on, status from bpmengine.historical_task
) task ON task.proc_inst_id = ins.id
LEFT JOIN (
	SELECT execution_id, proc_inst_id, 
    MAX( CASE WHEN NAME = 'category' THEN VALUE_STRING ELSE '' END) AS category, 
    MAX( CASE WHEN NAME = 'reason' THEN VALUE_STRING ELSE '' END) AS reason
    FROM bpmengine.run_execution_variable
    GROUP BY proc_inst_id, execution_id
	UNION
	SELECT execution_id, proc_inst_id, 
    MAX( CASE WHEN NAME = 'category' THEN VALUE_STRING ELSE '' END) AS category, 
    MAX( CASE WHEN NAME = 'reason' THEN VALUE_STRING ELSE '' END) AS reason
    FROM bpmengine.historical_execution_variable
    GROUP BY proc_inst_id, execution_id
) var ON var.proc_inst_id = ins.id and var.proc_inst_id = task.proc_inst_id
LEFT JOIN (
	SELECT ol.op_datetime, ol.username, ol.created_on, keystable.json_key, ol.process_instance_id, ol.reference_id, JSON_EXTRACT(json_extract(ol.payload,'$.entities'), CONCAT('$.', json_key, '.value')) json_value  FROM ai.OP_LOG ol,
	JSON_TABLE(JSON_KEYS(json_extract(ol.payload,'$.entities')),
					'$[*]' COLUMNS (json_key VARCHAR(255) PATH '$')) keystable
	WHERE OPTYPE = 'NER' AND USERNAME <> 'SYSTEM'
) op ON op.process_instance_id = task.proc_inst_id and op.process_instance_id = ins.id
WHERE mm.CREATED_ON BETWEEN '2023-01-01 00:00:00' AND '2023-03-28 23:59:59'
AND task.status = 'Completed' AND task.name NOT IN ('Pending AI', 'Awaiting Claim Creation', 'Survey', 'Do Task');
