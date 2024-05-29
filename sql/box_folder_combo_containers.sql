-- Containers with indicators suspected of being merged box:folder pairs
SELECT tc.id AS container_record_id,
       concat('/top_containers/', tc.id) as container_url,
       concat('/repositories/', tc.repo_id, '/top_containers/', tc.id) as api_url,
       ev1.value AS container_type,
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
       lp.depth AS location_depth
FROM top_container tc
LEFT JOIN enumeration_value ev1 ON tc.type_id = ev1.id
LEFT JOIN top_container_profile_rlshp tcpr ON tc.id = tcpr.top_container_id
LEFT JOIN container_profile cp ON tcpr.container_profile_id = cp.id
LEFT JOIN enumeration_value ev2 ON cp.dimension_units_id = ev2.id
LEFT JOIN top_container_link_rlshp tcl ON tc.id = tcl.top_container_id
LEFT JOIN top_container_housed_at_rlshp tch ON tc.id = tch.top_container_id
LEFT JOIN location l ON tch.location_id = l.id
LEFT JOIN location_profile_rlshp lpr ON l.id = lpr.location_id
LEFT JOIN location_profile lp ON lpr.location_profile_id = lp.id
LEFT JOIN enumeration_value ev3 ON lp.dimension_units_id = ev3.id
WHERE tc.indicator LIKE '%:%'
ORDER BY location_record_id, container_record_id
