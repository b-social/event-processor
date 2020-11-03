-- :name get-lock :? :1
SELECT pg_advisory_lock(:lock_id);

-- :name release-lock :? :1
select pg_advisory_unlock(:lock_id);