--
SELECT
  concat('https://archivessearch.lib.uconn.edu/staff/top_containers/', tc.id) as container_url,
  CASE
    WHEN tc.indicator LIKE 'data_value_missing%' AND sc.indicator_2 LIKE 'data_value_missing%' THEN 'both'
    WHEN tc.indicator LIKE 'data_value_missing%' THEN 'top_container_only'
    ELSE 'sub_container_only' END AS problem_in_record_type,
    tc.id as container_record_id,
    tc.indicator as container_indicator,
    sc.id as sub_container_record,
    sc.indicator_2 as sub_container_indicator
FROM top_container tc
LEFT JOIN top_container_link_rlshp tclr
     ON tc.id = tclr.top_container_id
LEFT JOIN sub_container sc
     ON sc.id = tclr.sub_container_id
WHERE tc.indicator LIKE 'data_value_missing%' OR sc.indicator_2 LIKE 'data_value_missing%'
ORDER BY top_container_id, sub_container_id;
