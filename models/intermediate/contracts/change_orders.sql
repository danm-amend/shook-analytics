with change_orders as (
    select 
    a.ACO,
    a.ACOItem,
    a.PMCo,
    trim(a.Project) as Project,
    a.Description,
    a.Status,
    a.ApprovedDate,
    -- a.PendingAmount,
    a.ApprovedAmt,
    trim(a.Contract) as Contract,
    a.ContractItem,
    a.Approved,
    a.ApprovedBy,
    a.FixedAmountYN,
    a.FixedAmount,
    b.ApprovedMonth,
    a.KeyID as pmoi_key_id,
    b.KeyID as jcoi_key_id,
    CASE
        when a.Approved = 'Y' and b.ACO is null then True
        WHEN a.Approved != 'Y' then TRUE
        else False
    END as is_pending,
    CASE 
        when a.Approved = 'Y' and b.ACO is null then a.ApprovedAmt
        when a.Approved != 'Y' then a.PendingAmount 
        else a.FixedAmount 
    END as pending_amount
from {{ source('shookdw', 'PMOI') }} as a 
left join 
    {{ source('shookdw', 'JCOI') }} as b 
on a.ACO = b.ACO and a.ACOItem = b.ACOItem and a.PMCo = b.JCCo and a.Project = b.Job
), deduped as (
    select *,
        row_number() over (partition by pmoi_key_id, jcoi_key_id order by ApprovedDate) as rn
    from change_orders
)

select * from deduped 
where rn = 1