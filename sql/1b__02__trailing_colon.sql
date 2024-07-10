-- Top containers with trailing colons should have them removed
SELECT id as container_id,
       indicator
FROM top_container
WHERE indicator LIKE '%:';
