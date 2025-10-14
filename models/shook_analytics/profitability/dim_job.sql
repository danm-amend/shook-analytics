with contract_master as (
    select * 
    from {{ ref('contract_master') }}
), orig_est as (
    select jcco, job, sum(estimated_cost) as orig_estimate 
    from {{ ref('cost_estimates') }}
    where trans_type = 'OE'
    group by jcco, job
), curr_est as (
    select jcco, job, sum(estimated_cost) as curr_estimate
    from {{ ref('cost_estimates') }}
    where trans_type in ('OE', 'CO')
    group by jcco, job
), proj_est as (
    select jcco, job, sum(estimated_cost) as proj_estimate
    from {{ ref('cost_estimates') }}
    where trans_type = 'PF'
    group by jcco, job
), actual_cost as (
    select 
        Jcco, Job, sum(actual_cost) as cost_to_date
    from
    {{ ref('actual_cost') }} 
    group by jcco, job
), committed_cost as (
    select 
        Jcco, Job, sum(committed_cost) as cost_to_date
    from
    {{ ref('committed_cost') }} 
    group by jcco, job
), change_orders as (
    select 
        pmco as jcco,
        Project as job,
        sum(case
            when is_pending = false then ApprovedAmt
            else 0  
        end) as approved_change_orders,
        sum(case
            when is_pending = true then pending_amount
            else 0
        end) as pending_change_orders
    from 
    {{ ref('change_orders') }}
    group by pmco, project
), accounts_rec as (
    select 
        jcco, contract as job
        , round(sum(Invoiced) - sum(paid) - sum(retainage), 2) as ar_balance 
    from 
    {{ ref('accounts_receivable') }}
    group by jcco, contract
), accounts_pay as (
    select 
    jcco, job, 
    sum(amount) as ap_balance
    from {{ ref('accounts_payable') }}
    where paid_date is null
    group by jcco, job
), header as(
    select 
        cm.JCCO,
        cm.contract,
        cm.job, 
        cm.Description,
        concat(cm.job, ' - ', cm.Description) as JobNumberName,
        cm.department,
        cm.contract_status,
        to_date(cm.start_date) as start_date, 
        to_date(coalesce(cm.ACTUAL_CLOSE_DATE, cm.MONTH_CLOSED)) as close_date,
        cm.cust_group, 
        cm.customer, 
        cm.pay_terms,
        cm.orig_contract_amt,
        cm.contract_amt,
        cm.billed_amt,
        cm.RECEIVED_AMT,
        cm.CURRENT_RETAIN_AMT,
        -- cm.contract_amt - cm.orig_contract_amt as approved_change_orders, 
        co.approved_change_orders,
        co.pending_change_orders,
        cm.orig_contract_amt - oe.orig_estimate as orig_gross_margin,
        cm.contract_amt - ce.curr_estimate as curr_gross_margin,
        cm.contract_amt - pe.proj_estimate as proj_gross_margin,
        DIV0((cm.orig_contract_amt - oe.orig_estimate), cm.orig_contract_amt)  as orig_gross_margin_pct,
        DIV0((cm.contract_amt - ce.curr_estimate), cm.contract_amt)  as curr_gross_margin_pct,
        DIV0((cm.contract_amt - pe.proj_estimate), cm.contract_amt)  as proj_gross_margin_pct,
        DIV0(ctd.cost_to_date, ce.curr_estimate) * cm.orig_contract_amt as earned_revenue,
        cm.billed_amt - (DIV0(ctd.cost_to_date, pe.proj_estimate) * cm.contract_amt) as over_under_billed,
        -- oc.collected_to_date,
        ar.ar_balance as ar_balance,
        -- -- ctd.cost_to_date,
        ap.ap_balance as ap_balance,
        (cm.billed_amt - ar.ar_balance - cm.CURRENT_RETAIN_AMT) - (ctd.cost_to_date - ap.ap_balance) as cash_flow,
        oe.orig_estimate,
        ce.curr_estimate,
        pe.proj_estimate,
        ctd.cost_to_date,
        pe.proj_estimate - ctd.cost_to_date as cost_to_complete,
        ce.curr_estimate - pe.proj_estimate as over_under,
        -- ctd.committed_cost,
        DIV0(ctd.cost_to_date, pe.proj_estimate) as percent_complete 
    from
    contract_master as cm
    left join 
    change_orders as co 
    using(jcco, job)
    left join 
    orig_est as oe
    using(jcco, job)
    left join 
    curr_est as ce 
    using(jcco, job)
    left join 
    proj_est as pe 
    using(jcco, job)
    left join 
    actual_cost as ctd 
    using(jcco, job)
    left join 
    accounts_pay as ap 
    using(jcco, job)
    left join 
    accounts_rec as ar 
    using(jcco, job)
    left join 
    committed_cost as cc 
    using(jcco, job)
)
select distinct *
from header
where start_date >= dateadd(year, -10, getdate())