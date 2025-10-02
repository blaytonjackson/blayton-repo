SELECT organization_group_id,
    STRING_AGG(permissions, ', ') AS group_permission_details
FROM (
        SELECT organization_group_id,
            CASE
                WHEN all_payers_access THEN 'All Payer Access'
                ELSE NULL
            END AS permissions
        FROM contracting_platform_clearcontractsscope
        UNION ALL
        SELECT organization_group_id,
            CASE
                WHEN all_providers_access THEN 'All Providers Access'
                ELSE NULL
            END AS permissions
        FROM contracting_platform_clearcontractsscope
        UNION ALL
        SELECT organization_group_id,
            CASE
                WHEN all_provider_entities_access THEN 'All Provider Entities Access'
                ELSE NULL
            END AS permissions
        FROM contracting_platform_clearcontractsscope
        UNION ALL
        SELECT organization_group_id,
            CASE
                WHEN all_plans_access THEN 'All Plans Access'
                ELSE NULL
            END AS permissions
        FROM contracting_platform_clearcontractsscope
        UNION ALL
        SELECT organization_group_id,
            CASE
                WHEN all_contract_types_access THEN 'All Contract Types Access'
                ELSE NULL
            END AS permissions
        FROM contracting_platform_clearcontractsscope
        UNION ALL
        SELECT organization_group_id,
            CASE
                WHEN all_labels_access THEN 'All Labels Access'
                ELSE NULL
            END AS permissions
        FROM contracting_platform_clearcontractsscope
        UNION ALL
        SELECT organization_group_id,
            CASE
                WHEN all_states_access THEN 'All States Access'
                ELSE NULL
            END AS permissions
        FROM contracting_platform_clearcontractsscope
        UNION ALL
        SELECT organization_group_id,
            CASE
                WHEN states IS NOT NULL THEN array_to_string(states, ',')
                ELSE NULL
            END AS permissions
        FROM contracting_platform_clearcontractsscope
    ) subquery
WHERE permissions IS NOT NULL
GROUP BY organization_group_id;