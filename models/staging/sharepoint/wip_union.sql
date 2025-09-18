with union_sales as (
    {{ union_wip_dynamic(source('metadata', 'wip_file_control'), 'shookdw', 'sharepoint') }}
)

select 
    *
from union_sales