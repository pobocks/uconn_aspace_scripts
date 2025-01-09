-- Containers with type "digitized" which, well, shouldn't ought to exist :-)
SELECT ao.id as archival_object_id,
       ao.title as archival_object_title,
       concat('https://archivessearch.lib.uconn.edu/staff/resources/', ao.root_record_id, '#tree::archival_object_', ao.id) as archival_object_url,
       concat('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as api_url,
       ev.value AS instance_type,
       tc.id AS container_record_id,
       tc.indicator AS container_indicator,
       tc.barcode AS container_barcode,
       ev_c.value AS container_type,
       sc.indicator_2 AS sub_container_indicator,
       concat('https://archivessearch.lib.uconn.edu/staff/top_containers/', tc.id) as container_url,
       count(i2.id) as number_of_other_instances,
       group_concat(tc2.id, ': ', ev2.value) as other_instance_container_ids
FROM instance i
LEFT JOIN archival_object ao ON ao.id = i.archival_object_id
JOIN enumeration_value ev ON i.instance_type_id = ev.id
LEFT JOIN sub_container sc ON sc.instance_id = i.id
LEFT JOIN top_container_link_rlshp tclr ON tclr.sub_container_id = sc.id
LEFT JOIN top_container tc ON tc.id = tclr.top_container_id
LEFT JOIN enumeration_value ev_c ON tc.type_id = ev_c.id
LEFT JOIN instance i2 ON i2.archival_object_id = i.archival_object_id
LEFT JOIN sub_container sc2 ON sc2.instance_id = i2.id
LEFT JOIN top_container_link_rlshp tclr2 ON tclr2.sub_container_id = sc.id
LEFT JOIN top_container tc2 ON tc2.id = tclr2.top_container_id
LEFT JOIN enumeration_value ev2 ON i2.instance_type_id = ev2.id
WHERE ev.value IN ('digitized', 'digitization in process')
  AND i.instance_type_id <> i2.instance_type_id
GROUP BY i.id
ORDER BY ao.id, i.id
