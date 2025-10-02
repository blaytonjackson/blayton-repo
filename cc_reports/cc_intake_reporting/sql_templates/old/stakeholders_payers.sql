--master inventory listing		
select distinct conmod_pay.name as payer
	, string_agg(distinct conplan.name, ', ') as plans 
	, conmod_pay.state as state
	, conmod_pay.ein 
	, string_agg(distinct conproj.name, ', ') as contract_name
from contracting_platform_contractproject conproj
inner join contracting_platform_legacynetworkagreement lna 
	on lna.legacycontractproject_ptr_id = conproj.id 
left join contracting_platform_legacynetworkagreement_payers lna_pay
	on lna.legacycontractproject_ptr_id = lna_pay.legacynetworkagreement_id 
left join contracting_platform_modeling_contractingpayer conmod_pay
	on conmod_pay.id = lna_pay.contractingpayer_id 
left join contracting_platform_modeling_contractingplan conplan 
	on conplan.payer_id = conmod_pay.id 
left join contracting_platform_legacynetworkagreement_provider_entities lna_proen
	on lna_proen.legacynetworkagreement_id = lna.legacycontractproject_ptr_id 
where conmod_pay.organization_id = {{ organization_id }}
group by conmod_pay.name
	, conmod_pay.state 
	, conmod_pay.ein ;