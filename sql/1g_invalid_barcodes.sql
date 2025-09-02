--
SELECT tc.id AS container_record_id,
       sc.id AS sub_container_record_id,
       CASE
         WHEN tc.id IS NULL THEN NULL
         ELSE concat('https://archivessearch.lib.uconn.edu/staff/top_containers/', tc.id)
       END AS container_url,
       CASE
         WHEN tc.id IS NULL THEN NULL
         ELSE concat('/repositories/', tc.repo_id, '/top_containers/', tc.id)
       END AS api_url,
       CASE
         WHEN i.resource_id IS NOT NULL THEN concat('/repositories/', tc.repo_id, '/resources/', i.resource_id)
         WHEN i.archival_object_id IS NOT NULL THEN concat('/repositories/', tc.repo_id, '/archival_objects/', i.archival_object_id)
         ELSE concat('/repositories/', tc.repo_id, '/accessions/', i.accession_id)
       END AS record_api_url,
       ev1.value AS container_type,
       tc.indicator AS container_indicator,
       concat_ws('; ', ev2.value, ev3.value) AS sub_container_types,
       concat_ws('; ', sc.indicator_2, sc.indicator_3) AS sub_container_indicators,
       CASE
         WHEN tc.barcode IS NOT NULL AND tc.barcode NOT REGEXP '^\\d{14}$' AND sc.barcode_2 IS NOT NULL AND sc.barcode_2 NOT REGEXP '^\\d{14}$' THEN 'both'
         WHEN tc.barcode IS NOT NULL AND tc.barcode NOT REGEXP '^\\d{14}$' THEN 'top_container'
         ELSE 'sub_container' 
       END AS problem_barcode,
       tc.barcode as top_container_barcode,
       sc.barcode_2 AS sub_container_barcode
FROM sub_container sc
LEFT JOIN top_container_link_rlshp tclr ON sc.id = tclr.sub_container_id
LEFT JOIN top_container tc ON tc.id = tclr.top_container_id
LEFT JOIN instance i ON sc.instance_id = i.id
LEFT JOIN enumeration_value ev1 ON tc.type_id = ev1.id
LEFT JOIN enumeration_value ev2 ON sc.type_2_id = ev2.id
LEFT JOIN enumeration_value ev3 ON sc.type_3_id = ev3.id
WHERE (sc.barcode_2 IS NOT NULL AND sc.barcode_2 NOT REGEXP '^\\d{14}$') OR (tc.barcode IS NOT NULL AND tc.barcode NOT REGEXP '^\\d{14}$') AND NOT (LOCATE('data_value_missing_', sc.indicator_2) > 0 OR LOCATE('data_value_missing_', tc.indicator) > 0) 
ORDER BY container_record_id ASC, sub_container_record_id ASC
