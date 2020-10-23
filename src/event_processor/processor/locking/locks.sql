-- :name get-lock :? :1
SELECT pg_advisory_lock(1);

-- :name release-lock :? :1
select pg_advisory_unlock(1);