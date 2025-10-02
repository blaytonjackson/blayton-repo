SELECT DISTINCT MIN(b.id) AS organization_id,
    b.name AS org_name
FROM public.tq_organizations_organization_products a
    JOIN public.tq_organizations_organization b ON a.organization_id = b.id
WHERE a.product_id = 2
    AND b.parent_id IS NULL
    AND b.is_active = true
GROUP BY b.name;