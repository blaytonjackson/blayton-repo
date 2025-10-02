select distinct MIN(organization_id) as organization_id,
    name,
    npi
from public.contracting_platform_modeling_contractingprovider cpmc
GROUP BY name,
    npi;