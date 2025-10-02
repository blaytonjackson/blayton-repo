--CTE to pull in maxchangeid
with maxchangeid as
	(
		select max(id) as id
			, project_id
			, document_id
		from contracting_platform_auditlog al 
		where change_description ilike %s
		group by project_id 
			, document_id 
	),
--CTE to pull in max change info for most recent DOC change
maxdocchange as
	(
		select al.project_id
			, al.document_id
			, al.user_id
			, date(al.last_updated) as change_date
		from contracting_platform_auditlog al
		inner join maxchangeid mc
			on al.id = mc.id
			and al.project_id = mc.project_id
	)
--doc inventory listing, limited to docs added to existing contracts in the last two weeks
select conproj.external_id as contract_external_id
	, conproj.name as contract_name 
	, hd.external_id as doc_external_id
	, hd.intake_status 
	, date(d.created) as doc_created_date
	, mdc.change_date as doc_change_date
	, extract(month from mdc.change_date) as doc_change_month
	, extract(year from mdc.change_date) as doc_change_year
	, contype.name as contract_type
	, string_agg(distinct conmod_pay.name, ', ') as payer
	, string_agg(distinct conproen.name, ', ') as provider_entity
	, string_agg(distinct conpro.state, ', ') as provider_state 
	, d.created_by_id ------------need to update to user name
	, hd.type as document_type
	, hd.effective_date as document_effective_date
	, hd.name as document_name
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
left join contracting_platform_legacynetworkagreement_org_contract_types lna_contype
	on lna.legacycontractproject_ptr_id = lna_contype.legacynetworkagreement_id 
left join contracting_platform_contracttype contype
	on contype.id = lna_contype.contracttype_id 
inner join maxdocchange mdc
	on mdc.project_id = conproj.id 
	and mdc.document_id = hd.document_ptr_id 
where d.organization_id = %s
	and date(conproj.created) != date(d.created) 
	and d.created >= current_date - interval '3 weeks'
group by hd.external_id 
	, hd.intake_status 
	, d.created 
	, conproj.external_id 
	, conproj.name
	, contype.name
	, hd.type
	, hd.effective_date 
	, hd.name 
	, mdc.change_date
	, d.created_by_id