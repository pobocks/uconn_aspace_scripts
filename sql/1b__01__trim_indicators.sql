-- Containers and sub_containers which have problematic indicators (whitespace)
SELECT tc.id as container_id,
       tc.indicator,
       sc.id as sub_container_id,
       i.id as instance_id,
       sc.indicator_2,
       sc.indicator_3,
       coalesce(r.id, ao.root_record_id) as resource_id,
       ao.id as archival_object_id
  FROM top_container tc
  JOIN top_container_link_rlshp tclr ON tc.id = tclr.top_container_id
  JOIN sub_container sc ON sc.id = tclr.sub_container_id
  JOIN instance i ON i.id = sc.instance_id
  LEFT JOIN resource r ON r.id = i.resource_id
  LEFT JOIN archival_object ao ON ao.id = i.archival_object_id
  WHERE indicator REGEXP '^[[:space:]]|[[:space:]]$'
     OR indicator_2 REGEXP '^[[:space:]]|[[:space:]]$'
     OR indicator_3 REGEXP '^[[:space:]]|[[:space:]]$'
  ORDER BY tc.id, sc.id;
