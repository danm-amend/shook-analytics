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
), add_first_of_month as (
    select 
        *, 
        date_trunc('MONTH', to_date(change_date)) as change_month
    from contract_to_change_order
)

select
    JCCO,
    contract,
    change_date,
    change_month,
    contract_amt
from add_first_of_month

