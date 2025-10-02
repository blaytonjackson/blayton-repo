-- CSH_CC_Tags by Individual Provider_120324.sql
select 
	conproj.external_id as contract_external_id
	, replace(replace(string_agg(distinct lna.state::text, ', '),'{',''),'}','') as state
	, conproj.name as contract_name
	, string_agg(distinct conmod_pay.name, ', ') as payer
	, conproen.name as provider_entity
	, conpro.name as provider
	, contype.name as contract_type
	, conproj.status 
	, conproj.effective_date 
	, conproj.termination_date 
	, lna.renewal_type 
	, lna.renewal_date 
	, lna.renewal_notice_deadline 
	, lna.notes 
	, string_agg(distinct conplan.name, ', ') as plans 
	, string_agg(distinct conproj_o.user_id::text, ', ') as contract_owners 
	, string_agg(distinct cpl.name, ', ') as labels
	, tt.title as tag
	, t.value as tag_value
	, t.associated_text 
	, hd.name as doc_name
	, hd.type as doc_type
from contracting_platform_contractproject conproj
inner join contracting_platform_document d
	on d.project_id = conproj.id 
inner join contracting_platform_hierarchicaldocument hd
	on hd.document_ptr_id = d.id 
left join contracting_platform_contractproject_labels conproj_l 
	on conproj.id = conproj_l.contractproject_id 
left join contracting_platform_label cpl 
	on cpl.id = conproj_l.label_id 
	and cpl.organization_id = conproj.originating_organization_id 
left join contracting_platform_contractproject_owners conproj_o
	on conproj.id = conproj_o.contractproject_id  
inner join contracting_platform_legacynetworkagreement lna 
	on lna.legacycontractproject_ptr_id = conproj.id 
left join contracting_platform_legacynetworkagreement_payers lna_pay
	on lna.legacycontractproject_ptr_id = lna_pay.legacynetworkagreement_id 
left join contracting_platform_modeling_contractingpayer conmod_pay
	on conmod_pay.id = lna_pay.contractingpayer_id 
left join contracting_platform_legacynetworkagreement_providers lna_pro
	on lna_pro.legacynetworkagreement_id = lna.legacycontractproject_ptr_id 
inner join contracting_platform_modeling_contractingprovider conpro
	on conpro.id = lna_pro.contractingprovider_id 
	and conpro.organization_id = conproj.originating_organization_id 
left join contracting_platform_legacynetworkagreement_provider_entities lna_proen
	on lna_proen.legacynetworkagreement_id = lna.legacycontractproject_ptr_id 
inner join contracting_platform_modeling_contractingproviderentity conproen
	on lna_proen.contractingproviderentity_id = conproen.id 
left join contracting_platform_legacynetworkagreement_plans lna_plan
	on lna.legacycontractproject_ptr_id = lna_plan.legacynetworkagreement_id 
	and conproj.originating_organization_id = conproen.organization_id 
left join contracting_platform_modeling_contractingplan conplan
	on conplan.payer_id = conmod_pay.id
inner join contracting_platform_tag t
	on t.document_id = hd.document_ptr_id 
inner join contracting_platform_tagtemplate tt
	on tt.id = t.tag_template_id 
left join contracting_platform_legacynetworkagreement_org_contract_types lna_contype
	on lna.legacycontractproject_ptr_id = lna_contype.legacynetworkagreement_id 
left join contracting_platform_contracttype contype
	on contype.id = lna_contype.contracttype_id 
where d.organization_id = {{ organization_id }}
group by conproj.external_id 
	, conproj.name 
	, conproen.name
	, conmod_pay.name
	, contype.name
	, conproj.effective_date 
	, conproj.termination_date
	, lna.renewal_type 
	, lna.renewal_date 
	, lna.renewal_notice_deadline 
	, lna.notes 
	, tt.title
	, t.value
	, t.associated_text 
	, hd.name
	, hd.type
	, conproj.status 
	, conpro.name;