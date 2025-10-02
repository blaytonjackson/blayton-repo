-- tag ids '64','65','68','70','89','686','164','801','73','69'

select conproj.name as Contract_Name
	, conproj.external_id as Contract_External_ID
	, hd.name as Document_Name
	, hd.type as Doc_Type
	, hd.external_id as Doc_External_ID 
	, hd.effective_date as Doc_Effective_Date 
	, case when t.value = ''
		then 'VALUE NOT FOUND'
		else t.value 
		end as Tag_Effective_Date
	, t.page_index as Tag_Page
	, tt.title as tag_title
	, 'https://hipaa.turquoise.health/contracting/'|| conproj.id ||'/hierarchical_language/'||t.document_id ||'/#page=1&zoom=auto,-31,800''' as doc_url
from contracting_platform_tag t 
inner join contracting_platform_tagtemplate tt
	on t.tag_template_id = tt.id 
inner join contracting_platform_document d 
	on t.document_id = d.id 
inner join contracting_platform_hierarchicaldocument hd 
	on hd.document_ptr_id = d.id 
left join contracting_platform_contractproject conproj
	on d.project_id = conproj.id 
where tt.id IN ('64','65','68','70','89','686','164','801','73','69')
	and d.organization_id = {{ organization_id }}
	and hd.effective_date::text != t.value ;