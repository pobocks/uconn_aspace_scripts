-- Archival Objects at "file" or "item" level without containers

SELECT ao.id as archival_object_record_id,
       concat('/resources/', ao.root_record_id, '#tree::archival_object_', ao.id) as archival_object_url,
       concat('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as api_url,
       ao.component_id,
       ao.ref_id,
       ao.root_record_id as resource_id,
       r.title AS resource_title,
       ao.parent_id,
       parent.title AS parent_title,
       ao.publish,
       tc_ev.value as prospective_container_type,
       tc.indicator as prospective_indicator,
       sc_ev1.value as prospective_subtype_2,
       sc.indicator_2 as prospective_indicator_2,
       sc_ev2.value as prospective_subtype_3,
       sc.indicator_3 as prospective_indicator_3
FROM archival_object ao
LEFT JOIN resource r ON r.id = ao.root_record_id
LEFT JOIN archival_object parent ON parent.id = ao.parent_id
LEFT JOIN instance i ON ao.id = i.archival_object_id
LEFT JOIN instance p_i on ao.parent_id = p_i.archival_object_id
LEFT JOIN sub_container sc ON sc.instance_id = p_i.id
LEFT JOIN top_container_link_rlshp tclr ON tclr.sub_container_id = sc.id
LEFT JOIN top_container tc ON tc.id = tclr.top_container_id
LEFT JOIN enumeration_value sc_ev1 ON sc.type_2_id = sc_ev1.id
LEFT JOIN enumeration_value sc_ev2 ON sc.type_3_id = sc_ev2.id
LEFT JOIN enumeration_value tc_ev ON tc.type_id = tc_ev.id
WHERE i.archival_object_id IS NULL
  AND (ao.level_id = 890 OR ao.level_id = 892)
