--
SELECT tc.id AS container_record_id,
       tc.indicator AS container_indicator,
       concat('https://archivessearch.lib.uconn.edu/staff/top_containers/', tc.id) as container_url,
       concat('/repositories/', tc.repo_id, '/top_containers/', tc.id) as api_url,
       sc.indicator_2 AS sub_container_indicator
FROM   sub_container sc
RIGHT OUTER JOIN   top_container_link_rlshp tclr ON tclr.sub_container_id = sc.id
RIGHT OUTER JOIN   top_container tc ON tc.id = tclr.top_container_id
WHERE  sc.indicator_2 REGEXP '(^|\s+|\\[)[1234567890]{14}([^0123456789]|$)'
OR     tc.indicator REGEXP '(^|\s+|\\[)[1234567890]{14}([^0123456789]|$)'
