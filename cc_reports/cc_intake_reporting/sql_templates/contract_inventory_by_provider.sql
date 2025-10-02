--CTE to pull in related contracts
with relatedcontract as 
		(
		select conproj.originating_organization_id as orgid
			, lna.legacycontractproject_ptr_id as from_contract_id
			, conproj."name" as from_contract_name
			, lna_rc.to_legacynetworkagreement_id as to_contract_id
			, conproj2."name" as to_contract_name
		from contracting_platform_legacynetworkagreement lna 
		inner join contracting_platform_legacynetworkagreement_related_contracts lna_rc  
			on lna.legacycontractproject_ptr_id = lna_rc.from_legacynetworkagreement_id 
		inner join contracting_platform_legacynetworkagreement lna_rc2
			on lna_rc.to_legacynetworkagreement_id = lna_rc2.legacycontractproject_ptr_id 
		inner join contracting_platform_contractproject conproj 
			on conproj.id = lna.legacycontractproject_ptr_id 
		inner join contracting_platform_contractproject conproj2
			on conproj2.id = lna_rc2.legacycontractproject_ptr_id 
		)
		
--master inventory listing		
select distinct conproj.external_id as contract_external_id
	, conproj.id as contract_id
	, replace(replace(string_agg(distinct lna.state::text, ', '),'{',''),'}','') as state
	, conproj."name" as contract_name
	, string_agg(distinct conmod_pay.name, ', ') as payer
	, conproen."name" as provider_entity
	, conpro.name as provider
	, contype."name" as contract_type
	, conproj.status as contract_status
	, lna.intake_status 
	, conproj.effective_date 
	, conproj.termination_date 
	, lna.renewal_type 
	, lna.renewal_date 
	, lna.renewal_notice_deadline 
	, lna.destruction_date 
	, lna.notes 
	, string_agg(distinct conplan.name, ', ') as plans 
	, string_agg(distinct conproj_o.user_id::text , ', ') as contract_owner ----need to replace with user name
	, string_agg(distinct conpro.npi, ', ') as npi 
	, string_agg(distinct conpro.ein, ', ') as ein 
	, string_agg(distinct rc.to_contract_name, ', ') as related_contracts 
	, lna.provider_signatory_name 
	, lna.payer_signatory_name 
	, lna.provider_signatory_title 
	, lna.payer_signatory_title 
	, string_agg(distinct cpl."name", ', ') as labels
from contracting_platform_contractproject conproj
left join contracting_platform_document d
	on d.project_id = conproj.id 
left join contracting_platform_hierarchicaldocument hd
	on hd.document_ptr_id = d.id 
left join contracting_platform_contractproject_labels conproj_l 
	on conproj.id = conproj_l.contractproject_id 
left join contracting_platform_label cpl 
	on cpl.id = conproj_l.label_id 
	and cpl.organization_id = conproj.originating_organization_id 
left join contracting_platform_contractproject_owners conproj_o
	on conproj.id = conproj_o.contractproject_id  
left join contracting_platform_legacynetworkagreement lna 
	on lna.legacycontractproject_ptr_id = conproj.id 
left join contracting_platform_legacynetworkagreement_org_contract_types lna_contype
	on lna.legacycontractproject_ptr_id = lna_contype.legacynetworkagreement_id 
left join contracting_platform_contracttype contype
	on contype.id = lna_contype.contracttype_id 
left join contracting_platform_legacynetworkagreement_payers lna_pay
	on lna.legacycontractproject_ptr_id = lna_pay.legacynetworkagreement_id 
left join contracting_platform_modeling_contractingpayer conmod_pay
	on conmod_pay.id = lna_pay.contractingpayer_id 
left join contracting_platform_legacynetworkagreement_providers lna_pro
	on lna.legacycontractproject_ptr_id = lna_pro.legacynetworkagreement_id 
left join contracting_platform_modeling_contractingprovider conpro
	on lna_pro.contractingprovider_id = conpro.id 
left join contracting_platform_legacynetworkagreement_plans lna_plan
	on lna.legacycontractproject_ptr_id = lna_plan.legacynetworkagreement_id 
left join contracting_platform_modeling_contractingplan conplan 
	on lna_plan.contractingplan_id = conplan.id
	and lna_pay.contractingpayer_id = conplan.payer_id 
left join relatedcontract rc 
	on rc.from_contract_id = lna.legacycontractproject_ptr_id 
	and rc.orgid = conproj.originating_organization_id 
left join contracting_platform_legacynetworkagreement_provider_entities lna_proen
	on lna_proen.legacynetworkagreement_id = lna.legacycontractproject_ptr_id 
left join contracting_platform_modeling_contractingproviderentity conproen
	on lna_proen.contractingproviderentity_id = conproen.id 
	and conproj.originating_organization_id = conproen.organization_id 
where conproj.originating_organization_id = {{ organization_id }}
and conproj.name not in ('tq test', 'TQ Test', 'Example')
group by conproj.external_id 
	, conproj.external_id
	, conproj."name"
	, conproen."name"
	, contype."name"
	, conproj.status 
	, conproj.effective_date 
	, conproj.termination_date 
	, lna.renewal_type 
	, lna.renewal_date 
	, lna.renewal_notice_deadline 
	, lna.destruction_date 
	, lna.notes 
	, lna.provider_signatory_name 
	, lna.payer_signatory_name 
	, lna.provider_signatory_title 
	, lna.payer_signatory_title 
	, conproj.id
	, conpro."name" 
	, lna.intake_status 