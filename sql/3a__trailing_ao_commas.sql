--
SELECT id,
       concat('https://archivessearch.lib.uconn.edu/staff/resources/', root_record_id, '#tree::archival_object_', id) as archival_object_url,
       concat('/repositories/', repo_id, '/archival_objects',  id) as archival_object_url,
       title,
       regexp_replace(title, ',\s*$', '') as proposed_title
FROM archival_object
WHERE title REGEXP ',\s*$';
