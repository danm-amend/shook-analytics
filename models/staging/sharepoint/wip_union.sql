with union_wip as (
    {{ union_tables_dynamic(source('metadata', 'wip_file_control'), 'shookdw', 'sharepoint') }}
)

select 
    *
from union_wip