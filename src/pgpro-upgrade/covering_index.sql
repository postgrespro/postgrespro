insert into pg_attribute
select 
attrelid, 'conincluding', atttypid, attstattarget, attlen, 25, attndims, attcacheoff, atttypmod, attbyval, attstorage, attalign, attnotnull, atthasdef, attisdropped, attislocal, attinhcount, attcollation, attacl, attoptions, attfdwoptions
from pg_attribute 
where attrelid = 'pg_constraint'::regclass::oid and attname ='conkey';
update pg_class set relnatts=25 where relname='pg_constraint';
