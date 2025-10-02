--CTE to pull in maxchangeid
with maxchangeid as
	(
		select max(id) as id
			, project_id
		from contracting_platform_auditlog al 
		where (change_description ilike %s ---check if we should include
				or change_description ilike %s)
		and document_id is null
		group by project_id 
			, document_id 
	),
	
--CTE to pull in max change info for most recent DOC change
maxconchange as
	(
		select al.project_id
			, al.document_id
			, al.user_id
			, date(al.last_updated) as change_date
		from contracting_platform_auditlog al
		inner join maxchangeid mc
			on al.id = mc.id
			and al.project_id = mc.project_id
	),
	
--CTE to pull in the contract creator info
contractcreator as
	(
		select  project_id 
			, user_id
			, created as contract_created_date
		from contracting_platform_auditlog al 
		where change_description = 'created the Legacy Network Agreement'
		and document_id is null
	)

--doc inventory listing, limited to docs added to existing contracts in the last two weeks
select 
	conproj.external_id as contract_external_id
	, lna.intake_status 
	, date(conproj.created) as contract_created_date
	, mcc.change_date as contract_change_date
	, extract(month from mcc.change_date) as contract_change_month
	, extract(year from mcc.change_date) as contract_change_year
	, conproj.name as contract_name
	, contype.name as contract_type
	, string_agg(distinct conmod_pay.name, ', ') as payer
	, string_agg(distinct conproen.name, ', ') as provider_entity
	, string_agg(distinct conpro.state, ', ') as provider_state 
	, mcc.user_id as last_change_name ---need to replace with user name & email
	, cc.user_id as contract_creator_name ---need to replace with user name & email
	, conproj.effective_date as contract_effective_date
from contracting_platform_contractproject conproj
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
inner join maxconchange mcc
	on mcc.project_id = conproj.id 
left join contractcreator cc
	on cc.project_id = conproj.id
where conproj.originating_organization_id = %s
	and conproj.created >= current_date - interval '3 weeks'
group by conproj.external_id 
	, conproj.name
	, contype.name
	, mcc.change_date
	, lna.intake_status 
	, conproj.created
	, conproj.effective_date
	, cc.user_id
	, mcc.user_id;