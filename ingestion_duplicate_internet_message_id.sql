SELECT mm.id, mm.subject, mm.status, mm.created_on ingested_date, mm.reference_id ingestion_ref_id, ins.business_key case_ref_id, ins.id process_instance_id, ins.created_on case_created_on FROM ingestion.mailmessage mm
LEFT JOIN (
	select id, business_key, created_on from bpmengine.run_proc_inst
	union
	select id, business_key, created_on from bpmengine.historical_proc_inst
) ins ON ins.business_key = mm.reference_id
WHERE mm.CREATED_ON BETWEEN '2023-03-01 00:00:00' AND '2023-03-28 23:59:59';

select md.id, 
md.usermailid, 
md.reference_id,
mm.reference_id processed_mail_reference_id,
md.subject duplicate_mail_subject, 
mm.subject processed_mail_subject, 
md.internet_message_id duplicate_message_id, 
mm.internet_message_id processed_message_id, 
md.status ingestion_status, 
md.created_by, 
md.created_on, 
md.updated_on, 
md.attachments_count, 
md.sender_address, 
md.received_on, md.retry_count, 
md.duplicate_message_id 
from ingestion.mailmessage md
join ingestion.mailmessage mm on mm.id = md.duplicate_message_id
where md.status = 'Duplicate'
and md.subject = mm.subject
