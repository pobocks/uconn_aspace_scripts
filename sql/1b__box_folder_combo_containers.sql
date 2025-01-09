-- Containers with indicators suspected of being merged box:folder pairs
SELECT main.*,
       GROUP_CONCAT(DISTINCT COALESCE(candidate_direct.id, candidate_ao.id) SEPARATOR ',') AS candidate_container_ids,
       GROUP_CONCAT(DISTINCT COALESCE(candidate_direct.barcode, candidate_ao.barcode) SEPARATOR ',') AS candidate_container_barcodes,
       GROUP_CONCAT(DISTINCT COALESCE(candidate_direct.indicator, candidate_ao.indicator, SUBSTRING_INDEX(container_indicator, ':', 1)) SEPARATOR ',') AS candidate_indicator
FROM (SELECT tc.id AS container_record_id,

       concat('https://archivessearch.lib.uconn.edu/staff/top_containers/', tc.id) as container_url,
       concat('/repositories/', tc.repo_id, '/top_containers/', tc.id) as api_url,
       ao.id AS archival_object_id,
       ev1.value AS container_type,
       tc.type_id AS container_type_id,
       tc.indicator AS container_indicator,
       tc.barcode AS container_barcode,
       tc.ils_holding_id AS container_ils_holding_id,
       tc.ils_item_id AS container_ils_item_id,
       tc.exported_to_ils AS container_exported_to_ils,
       cp.id AS container_profile_record_id ,
       cp.name AS container_profile,
       cp.extent_dimension AS container_profile_extent_dimension,
       ev2.value AS container_profile_dimension_units,
       cp.height AS container_height,
       cp.width AS container_width,
       cp.depth AS container_depth,
       cp.stacking_limit AS container_stacking_limit,
       l.id AS location_record_id,
       l.building AS location_building,
       l.floor AS location_floor,
       l.room AS location_room,
       l.area AS location_area,
       l.barcode AS location_barcode,
       l.classification AS location_classification,
       l.coordinate_1_label AS coordinate_1_label,
       l.coordinate_1_indicator AS coordinate_1_indicator,
       l.coordinate_2_label AS coordinate_2_label,
       l.coordinate_2_indicator AS coordinate_2_indicator,
       l.coordinate_3_label AS coordinate_3_label,
       l.coordinate_3_indicator AS coordinate_3_indicator,
       l.temporary_id AS location_is_temporary,
       lp.id AS location_profile_record_id,
       lp.name AS location_profile,
       ev3.value AS location_profile_dimension_units,
       lp.height AS location_height,
       lp.width AS location_width,
       lp.depth AS location_depth,
       COALESCE(r.id, ao.root_record_id) AS resource_id
     FROM top_container tc
LEFT JOIN enumeration_value ev1 ON tc.type_id = ev1.id
LEFT JOIN top_container_profile_rlshp tcpr ON tc.id = tcpr.top_container_id
LEFT JOIN container_profile cp ON tcpr.container_profile_id = cp.id
LEFT JOIN enumeration_value ev2 ON cp.dimension_units_id = ev2.id
LEFT JOIN top_container_housed_at_rlshp tch ON tc.id = tch.top_container_id
LEFT JOIN location l ON tch.location_id = l.id
LEFT JOIN location_profile_rlshp lpr ON l.id = lpr.location_id
LEFT JOIN location_profile lp ON lpr.location_profile_id = lp.id
LEFT JOIN enumeration_value ev3 ON lp.dimension_units_id = ev3.id
LEFT JOIN top_container_link_rlshp tclr ON tc.id = tclr.top_container_id
LEFT JOIN sub_container sc ON sc.id = tclr.sub_container_id
LEFT JOIN instance i ON i.id = sc.instance_id
LEFT JOIN archival_object ao ON i.archival_object_id = ao.id
LEFT JOIN resource r ON i.resource_id = r.id
WHERE tc.indicator REGEXP '^[[:alnum:]-]+:[[:alnum:]-]+$'
GROUP BY tc.id) main
LEFT JOIN resource r ON main.resource_id = r.id
LEFT JOIN instance i ON r.id = i.resource_id
LEFT JOIN sub_container sc ON sc.instance_id = i.id
LEFT JOIN top_container_link_rlshp tclr ON tclr.sub_container_id = sc.id
LEFT JOIN top_container candidate_direct ON tclr.top_container_id = candidate_direct.id AND candidate_direct.type_id IN (317, 11435) AND candidate_direct.indicator = SUBSTRING_INDEX(main.container_indicator, ':', 1)
LEFT JOIN archival_object ao ON ao.root_record_id = r.id
LEFT JOIN instance i2 ON ao.id = i2.archival_object_id
LEFT JOIN sub_container sc2 ON sc2.instance_id = i2.id
LEFT JOIN top_container_link_rlshp tclr2 ON tclr2.sub_container_id = sc2.id
LEFT JOIN top_container candidate_ao ON candidate_ao.id = tclr2.top_container_id AND candidate_ao.type_id IN (317, 11435) AND candidate_ao.indicator = SUBSTRING_INDEX(main.container_indicator, ':', 1)
GROUP BY main.container_record_id ASC, candidate_indicator ASC;
