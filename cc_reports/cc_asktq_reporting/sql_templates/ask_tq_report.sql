select distinct 
	  question 	
	, response 
	, conproj."name" as contract_name
	, string_agg(distinct cph.name, ', ') as Document_name
	, reference_folder 
	, string_agg (distinct cpr."name", ', ') as Reference_name
	, string_agg (distinct cpr.file, ', ') as reference_file_name
	, cpa.user_id as tqask_created_by_id --turn this into a name
	, cpa.created as created_at
	, cpa2.is_positive 
	, cpa2."comment" 
from contracting_platform_asktqresponse cpa 
left join contracting_platform_asktqresponsefeedback cpa2 
	on cpa.id = cpa2.response_id 
left join contracting_platform_contractproject conproj 
	on cpa.contract_project_id = conproj.id 
	and cpa.organization_id = conproj.originating_organization_id 
left join contracting_platform_asktqresponse_documents cpad 
	on cpa.id = cpad.asktqresponse_id 
left join contracting_platform_hierarchicaldocument cph 
	on cpad.hierarchicaldocument_id = cph.document_ptr_id 
left join contracting_platform_asktqresponse_references cpar 
	on cpa.id = cpar.asktqresponse_id 
left join contracting_platform_reference cpr 
	on cpar.reference_id = cpr.id 
where cpa.organization_id = {{ organization_id }}
group by question 	
	, response 
	, reference_folder 
	, conproj."name" 
	, user_id 
	, cpa.created 
	, cpa2.is_positive 
	, cpa2."comment" 
order by cpa.created DESC