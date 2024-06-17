--
SELECT resource_title,
       resource_id,
       indicator,
       count(top_container_id) AS count_with_this_indicator,
       group_concat(top_container_id SEPARATOR ', ') AS tc_ids,
       group_concat(linked_to SEPARATOR ', ') AS linked_tos
FROM (
     SELECT DISTINCT r_direct.title AS resource_title,
            r_direct.id AS resource_id,
            tc_direct.indicator AS indicator,
            tc_direct.id AS top_container_id,
            'resource' AS linked_to
     FROM resource r_direct
     JOIN instance i_d ON i_d.resource_id = r_direct.id
     JOIN sub_container sc_d ON sc_d.instance_id = i_d.id
     JOIN top_container_link_rlshp tclr_d ON tclr_d.sub_container_id = sc_d.id
     JOIN top_container tc_direct ON tc_direct.id = tclr_d.top_container_id
     UNION ALL
     SELECT DISTINCT r_ao.title AS resource_title,
            r_ao.id AS resource_id,
            tc_ao.indicator AS indicator,
            tc_ao.id AS top_container_id,
            'archival object' AS linked_to
     FROM resource r_ao
     JOIN archival_object ao ON ao.root_record_id = r_ao.id
     JOIN instance i_ao ON i_ao.archival_object_id = ao.id
     JOIN sub_container sc_ao ON sc_ao.instance_id = i_ao.id
     JOIN top_container_link_rlshp tclr_ao ON tclr_ao.sub_container_id = sc_ao.id
     JOIN top_container tc_ao ON tc_ao.id = tclr_ao.top_container_id
) containers
GROUP BY resource_id, resource_title, indicator
HAVING count_with_this_indicator > 1
ORDER BY resource_id, indicator;
