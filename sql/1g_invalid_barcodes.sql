--
SELECT 'top_container' AS record_type,
       tc.id as container_record_id,
       NULL AS sub_container_record_id,
       concat('https://archivessearch.lib.uconn.edu/staff/top_containers/', tc.id) as container_url,
       concat('/repositories/', tc.repo_id, '/top_containers/', tc.id) as api_url,
       ev.value AS container_type,
       tc.indicator AS container_indicator,
       NULL AS sub_container_types,
       NULL AS sub_container_indicators,
       tc.barcode AS barcode
FROM top_container tc
LEFT JOIN enumeration_value ev ON tc.type_id = ev.id
WHERE tc.barcode IS NOT NULL AND tc.barcode NOT REGEXP '\d{14}'
UNION
SELECT 'sub_container' AS record_type,
       tc2.id AS container_record_id,
       sc.id AS sub_container_record_id,
       CASE
         WHEN tc2.id IS NULL THEN NULL
         ELSE concat('https://archivessearch.lib.uconn.edu/staff/top_containers/', tc2.id)
       END AS container_url,
       CASE
         WHEN tc2.id IS NULL THEN NULL
         ELSE concat('/repositories/', tc2.repo_id, '/top_containers/', tc2.id)
       END AS api_url,
       ev1.value AS container_type,
       tc2.indicator AS container_indicator,
       concat_ws('; ', ev2.value, ev3.value) AS sub_container_types,
       concat_ws('; ', sc.indicator_2, sc.indicator_3) AS sub_container_indicators,
       sc.barcode_2 AS barcode
FROM sub_container sc
LEFT JOIN top_container_link_rlshp tclr ON sc.id = tclr.sub_container_id
LEFT JOIN top_container tc2 ON tc2.id = tclr.top_container_id
LEFT JOIN enumeration_value ev1 ON tc2.type_id = ev1.id
LEFT JOIN enumeration_value ev2 ON sc.type_2_id = ev2.id
LEFT JOIN enumeration_value ev3 ON sc.type_3_id = ev3.id
WHERE sc.barcode_2 IS NOT NULL AND sc.barcode_2 NOT REGEXP '\d{14}'
ORDER BY container_record_id ASC, record_type DESC, sub_container_record_id ASC
