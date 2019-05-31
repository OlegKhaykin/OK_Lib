select
  a.comp_id,
  NVL(dmad3.comp_name, NVL(dmadev.comp_name, NVL(venus.comp_name, NVL(dmaqa.comp_name, NVL(dmaperf.comp_name, dmaprod.comp_name))))) comp_name,
  NVL(dmad3.version, 'Missing') dmad3,
  NVL(dmad2.version, 'Missing') dmad2,
  NVL(venus.version, 'Missing') venus,
  NVL(dmadev.version, 'Missing') dmadev,
  NVL(dmaqa.version, 'Missing') dmaqa,
  NVL(dmaperf.version, 'Missing') dmaperf,
  NVL(dmaprod.version, 'Missing') dmaprod
from
  (
    select comp_id from dba_registry
    union
    select comp_id from dba_registry@dmad2
    union
    select comp_id from dba_registry@dmadev
    union
    select comp_id from dba_registry@venus
    union
    select comp_id from dba_registry@dmaqa
    union
    select comp_id from dba_registry@dmaperf
    union
    select comp_id from dba_registry@dmaprod
  ) a,
  dba_registry dmad3,
  dba_registry@dmad2 dmad2,
  dba_registry@venus venus,
  dba_registry@dmadev dmadev,
  dba_registry@dmaqa dmaqa,
  dba_registry@dmaperf dmaperf,
  dba_registry@dmaprod dmaprod
where dmad3.comp_id(+) = a.comp_id
and dmad2.comp_id(+) = a.comp_id
and dmadev.comp_id(+) = a.comp_id
and venus.comp_id(+) = a.comp_id
and dmaqa.comp_id(+) = a.comp_id
and dmaperf.comp_id(+) = a.comp_id
and dmaprod.comp_id(+) = a.comp_id
