with contracts as (
    select 
        JCCO, 
        contract,
        start_month as change_date,
        ORIG_CONTRACT_AMT as contract_amt 
    from {{ ref('contract_master') }}
), change_orders as (
    select 
        PMCo as JCCO,
        Project as contract,
        ApprovedDate as change_date,
        ApprovedAmt as contract_amt
    from {{ ref('change_orders') }}
    where is_pending = False
), contract_to_change_order as (
    select 
        *
    from contracts 
    union all 
    select 
        * 
    from change_orders 
)

select
-- * 
sum(contract_amt) 
from contract_to_change_order
where contract = '124016.' 

