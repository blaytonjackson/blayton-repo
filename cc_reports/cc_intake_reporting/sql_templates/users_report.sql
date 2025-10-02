SELECT au.id,
    au.first_name,
    au.last_name,
    au.email,
    ou.department,
    STRING_AGG(CONCAT(org.role, ' - ', op.name), ', ') AS cc_role_permissions,
    ou.role AS user_title,
    oog.name AS group_name,
    au.last_login,
    oogou.organization_group_id
FROM auth_user au
    INNER JOIN tq_organizations_organizationuser ou ON au.id = ou.user_id
    LEFT JOIN tq_organizations_organizationuser_groups ooug ON ooug.organizationuser_id = ou.id
    LEFT JOIN tq_organizations_rolegroup org ON org.group_ptr_id = ooug.group_id
    LEFT JOIN tq_organizations_product op ON op.id = org.product_id
    LEFT JOIN tq_organizations_organizationgrouporganizationuser oogou ON oogou.organization_user_id = ou.id
    LEFT JOIN tq_organizations_organizationgroup oog ON oog.organization_id = ou.organization_id
    AND oog.id = oogou.organization_group_id
WHERE (ou.organization_id = {{ organization_id }}
    AND au.is_staff != 'TRUE'
    AND ou.deleted_at IS NULL
    AND op.name IN('Clear Contracts', 'Organization Permission'))
    OR (au.email ILIKE '%turquoise.health')
GROUP BY au.id,
    au.first_name,
    au.last_name,
    ou.department,
    ou.role,
    au.last_login,
    oog.name,
    oog.description,
    au.email,
    oogou.organization_group_id;