select distinct conpro.name as facility_name
	, conpro.abbreviation 
	, conpro.provider_type 
	, conpro.ein 
	, conpro.npi 
	, conpro.address 
	, conpro.address_line_two 
	, conpro.city 
	, conpro.zip
	, conpro.state 
	, string_agg(distinct conproj.name, ', ') as contracts
	, conpro.medicare_ipps_exempt 
	, conpro.directly_employs_physicians
	, conpro.previous_doing_business_as
from contracting_platform_modeling_contractingprovider conpro
left join contracting_platform_legacynetworkagreement_providers lnacp
	on lnacp.contractingprovider_id = conpro.id 
left join contracting_platform_contractproject conproj
	on conproj.id = lnacp.legacynetworkagreement_id 
where conpro.organization_id = {{ organization_id }}
group by conpro.name
	, conpro.abbreviation 
	, conpro.provider_type 
	, conpro.npi 
	, conpro.medicare_ipps_exempt 
	, conpro.directly_employs_physicians 
	, conpro.ein 
	, conpro.address 
	, conpro.address_line_two 
	, conpro.state 
	, conpro.city
	, conpro.zip
	, conpro.previous_doing_business_as 
order by conpro."name" 