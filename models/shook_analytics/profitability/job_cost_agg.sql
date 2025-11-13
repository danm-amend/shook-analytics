WITH 
    -- job_bounds AS (
    --   SELECT 
    --     Jcco
    --     ,Job
    --     ,COALESCE(cm.START_DATE, cm.START_MONTH, ac.min_month) as min_month
    --     ,COALESCE(cm.ACTUAL_CLOSE_DATE, cm.MONTH_CLOSED, ac.max_month) as max_month
    --   from {{ ref('contract_master') }} as cm
    --   left join 
    --   (
    --     select Jcco, Job
    --     ,MIN(DATE_TRUNC('MONTH', CAST(Mth AS DATE))) AS min_month
    --     ,MAX(DATE_TRUNC('MONTH', CAST(Mth AS DATE))) AS max_month
    --     FROM {{ ref('actual_cost') }}
    --     GROUP BY jcco, job
    --   ) as ac 
    --   using(jcco, job)
    -- ),
    job_bounds AS (
        select 
            JCCO
            , Job
            ,MIN(DATE_TRUNC('MONTH', CAST(Mth AS DATE))) AS min_month
            ,MAX(DATE_TRUNC('MONTH', CAST(Mth AS DATE))) AS max_month
        FROM 
            (
                SELECT JCCO, Job, Mth
                FROM {{ ref('actual_cost') }}
                UNION
                SELECT JCCO, Job, Mth
                FROM {{ ref('cost_estimates') }}
                UNION
                SELECT JCCO, Job, Mth
                FROM {{ ref('committed_cost') }}
            )
        GROUP BY 
            JCCO
            , Job
    ),
    current_job_bounds AS (
        SELECT *
        FROM job_bounds
        WHERE min_month >= DATE_TRUNC('MONTH', DATEADD(YEAR, -8, CURRENT_DATE))
    ), 
    month_numbers AS (
      SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS month_offset
      FROM TABLE(GENERATOR(ROWCOUNT => 200))  -- enough months to cover any job span
    ),
    job_months AS (
      SELECT
        JCCO 
        ,cpb.Job
        ,DATEADD(MONTH, mn.month_offset, cpb.min_month) AS cost_month
      FROM current_job_bounds cpb
          JOIN month_numbers mn
            ON mn.month_offset <= DATEDIFF(MONTH, cpb.min_month, cpb.max_month)
    ),
    job_phases AS (
      SELECT DISTINCT JCCO, Job, Cost_Type, Phase_Group, Phase
      FROM {{ ref('phases') }}
    ),
    job_cost_months AS (
      SELECT 
        distinct 
        jp.JCCO
        ,jp.Job
        ,jp.Cost_Type
        ,jp.Phase_Group
        ,jp.Phase
        ,jm.cost_month as Month_Date
      FROM job_phases jp
          JOIN job_months jm
            ON jp.JCCO = jm.JCCO and jp.Job = jm.Job
    ), --select * from job_cost_months where job = '123040.' order by phase, cost_type, month_date
    --select count(*) from job_cost_months where job = '123040.'
    estimates_agg AS (
        SELECT
            --"PostedDate"
            JCCO
            ,Job
            ,Cost_Type
            ,Phase_Group
            ,Phase
            ,CAST(Mth AS DATE) AS Month_Date
            ,SUM(CASE WHEN trans_type = 'OE' THEN estimated_cost ELSE 0 END) AS original_estimate
            ,SUM(CASE WHEN trans_type in ('OE', 'CO') THEN estimated_cost ELSE 0 END) AS current_estimate
            ,SUM(CASE WHEN trans_type = 'PF' THEN estimated_cost ELSE 0 END) AS projected_estimate
        FROM 
            {{ ref('cost_estimates') }}

        GROUP BY
            JCCO
            ,Job
            ,Cost_Type
            ,Phase_Group
            ,Phase
            ,CAST(Mth AS DATE)
    ),-- select sum(projected_estimate) from estimates_agg where job = '123040.'
    actual_agg as (
        SELECT
            JCCO
            ,Job
            ,Cost_Type
            ,Phase_Group
            ,Phase
            ,CAST(Mth AS DATE) AS Month_Date
            , sum(actual_cost) as actual_cost
        FROM 
            {{ ref('actual_cost') }}

        GROUP BY
            JCCO
            ,Job
            ,Cost_Type
            ,Phase_Group
            ,Phase
            ,CAST(Mth AS DATE)
    ), 
    committed_agg as (
        SELECT
            JCCO
            ,Job
            ,Cost_Type
            ,Phase_Group
            ,Phase
            ,CAST(Mth AS DATE) AS Month_Date
            , sum(committed_cost) as committed_cost
        FROM 
            {{ ref('committed_cost') }}

        GROUP BY
            JCCO
            ,Job
            ,Cost_Type
            ,Phase_Group
            ,Phase
            ,CAST(Mth AS DATE)
    ),
    jc_months_filled AS (
        SELECT 
            jcm.JCCO
            ,jcm.Job
            ,jcm.Cost_Type
            ,jcm.Phase_Group
            ,jcm.Phase
            ,jcm.Month_Date
            ,COALESCE(ea.original_estimate, 0) AS "OriginalEst"
            ,COALESCE(ea.current_estimate, 0) "CurrentEst"
            ,COALESCE(ea.projected_estimate, 0) AS "Projected"
            ,COALESCE(aa.actual_cost, 0) AS "ActualCost"
            ,COALESCE(ca.committed_cost, 0) AS "CommittedCost"
        FROM job_cost_months jcm
          LEFT JOIN estimates_agg ea
          using(JCCO, Job, Cost_Type, Phase_Group, Phase, Month_Date)
          left JOIN actual_agg aa 
          using(JCCO, Job, Cost_Type, Phase_Group, Phase, Month_Date)
          LEFT JOIN committed_agg ca 
          using(JCCO, Job, Cost_Type, Phase_Group, Phase, Month_Date)
            -- ON jcm."Job" = jca."Job"
            -- AND jcm."CostType" = jca."CostType"
            -- AND jcm."PhaseGroup" = jca."PhaseGroup"
            -- AND jcm."Phase" = jca."Phase"
            -- AND jcm."cost_month" = jca."MonthDate"
    ),
    /*
    jc_cleanup AS (
        SELECT 
            "Job"
            ,"CostType"
            ,"PhaseGroup"
            ,"Phase"
        FROM
            jc_months_filled
        GROUP BY
            "Job"
            ,"CostType"
            ,"PhaseGroup"
            ,"Phase"
        HAVING
            SUM("OriginalEst") + SUM("CurrentEst") + SUM("Projected") + SUM("ActualCost") + SUM("CommittedCost") != 0
    ),
    */
    job_cost_detail AS (
        SELECT
            jmf.JCCO
            ,jmf.Job
            ,jmf.Month_Date as "cost_month"
            ,jmf.Cost_Type AS "CostTypeNumber"
            ,ct."Description" AS "CostTypeName"
            ,jmf.Phase_Group
            ,jmf.Phase
            ,SUBSTR(jmf.Phase, 1, 11) AS phase_core
            ,SUBSTR(jmf.Phase, 1,2) AS "Division"
            ,jp."Description"
            
            ,jmf."OriginalEst"
            ,SUM(jmf."OriginalEst") OVER (
                PARTITION BY jmf.Job, jmf.cost_type, jmf.phase_group, jmf.Phase
                ORDER BY jmf.Month_Date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeOriginalEst"
            
            ,jmf."CurrentEst"
            ,SUM(jmf."CurrentEst") OVER (
                PARTITION BY jmf.Job, jmf.cost_type, jmf.phase_group, jmf.phase
                ORDER BY jmf.Month_Date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeCurrentEst"
            
            ,jmf."Projected"
            ,SUM(jmf."Projected") OVER (
                PARTITION BY jmf.Job, jmf.cost_type, jmf.phase_group, jmf.phase
                ORDER BY jmf.Month_Date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeProjected"
            
            ,jmf."ActualCost"
            ,SUM(jmf."ActualCost") OVER (
                PARTITION BY jmf.Job, jmf.cost_type, jmf.phase_group, jmf.Phase
                ORDER BY jmf.month_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeActualCost"
            
            ,jmf."CommittedCost"
            ,SUM(jmf."CommittedCost") OVER (
                PARTITION BY jmf.Job, jmf.cost_type, jmf.phase_group, jmf.Phase
                ORDER BY jmf.month_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeCommittedCost"
            
        FROM jc_months_filled AS jmf
            LEFT JOIN {{ source('shookdw', 'bjcjp') }} AS jp
                ON jmf.jcco = jp."JCCo"
                and jmf.Job = jp."Job"
                and jmf.phase_group = jp."PhaseGroup"
                AND jmf.Phase = jp."Phase"
            LEFT JOIN {{ source('shookdw', 'bjcct') }} AS ct
                ON jmf.Phase_Group = ct."PhaseGroup"
                AND jmf.Cost_Type = ct."CostType"
            /*INNER JOIN jc_cleanup AS jcc
                ON jmf."Job" = jcc."Job"
                AND jmf."CostType" = jcc."CostType"
                AND jmf."PhaseGroup" = jcc."PhaseGroup"
                AND jmf."Phase" = jcc."Phase"*/
    ),

    add_metric_indicators AS (
        SELECT
            *
            ,CASE
                WHEN "Division" not in ('80', '96', '97', '98')
                    AND ROUND("CumulativeProjected" - "CumulativeActualCost", 0) < 0 THEN 1
                ELSE 0
            END AS "NegativeCTCIndicator"
            ,CASE
                WHEN "Division" not in ('80', '96', '97', '98')
                    AND CAST("CostTypeNumber" AS varchar) in ('2', '3')
                    AND "CumulativeCommittedCost" > 0 
                    AND "CumulativeActualCost" < "CumulativeProjected"
                    AND "CumulativeProjected" >= "CumulativeCommittedCost" * 1.05 THEN 1
                ELSE 0
            END AS "POVarianceOppIndicator"
            ,CASE
                WHEN "Division" in ('96', '97', '98') AND "CumulativeActualCost" > "CumulativeProjected" THEN 1
                ELSE 0
            END AS "ProfitAtRiskIndicator"
        FROM
            job_cost_detail
    ),
    add_metric_calcs AS (
        SELECT
            *
            ,CASE
                WHEN "NegativeCTCIndicator" = 1 THEN ROUND("CumulativeProjected" - "CumulativeActualCost", 2)
                ELSE 0
            END AS "NegativeCTCValue"
            ,CASE
                WHEN "POVarianceOppIndicator" = 1 THEN ROUND("CumulativeProjected" - "CumulativeCommittedCost" + LEAST(0, "CumulativeCommittedCost" - "CumulativeActualCost"), 2)
                ELSE 0
            END AS "POVarianceOppValue"
            ,CASE
                WHEN "ProfitAtRiskIndicator" = 1 THEN ROUND("CumulativeActualCost" - "CumulativeProjected", 2)
                ELSE 0
            END AS "ProfitAtRiskValue"
        FROM add_metric_indicators
    ) 
    ,add_descriptions as (
        SELECT
            *
            ,concat("CostTypeNumber", ' - ', "CostTypeName") as "CostTypeDescription"
            ,concat("PHASE", ' - ', "Description") as "PhaseDescription"
            /*
            ,CASE
                WHEN "PHASE" = '801000.901.' THEN 'Profit_On_CO'
                WHEN "Division" = '80' THEN 'General'
                ELSE 'Other'
            END AS "SupplementalConditions"
            */
            ,CASE
                WHEN "PHASE_CORE" = '801000.900.' THEN 'Supplemental Conditions'
                WHEN "PHASE_CORE" = '801000.901.' THEN 'Internal Contract Contingency'
                WHEN "PHASE_CORE" = '801000.902.' THEN 'Economic Contingency'
                WHEN "PHASE_CORE" = '801000.903.' THEN 'Labor Goal'
                WHEN "PHASE_CORE" = '801000.904.' THEN 'Profit on Change Orders'
                WHEN "PHASE_CORE" = '801000.905.' THEN 'Profit on Bonds and Insurance'
                WHEN "PHASE_CORE" = '801000.906.' THEN 'Profit on Unit Prices'
                WHEN "PHASE_CORE" = '801000.907.' THEN 'Profit on Billable Rates'
                WHEN "PHASE_CORE" = '801000.908.' THEN 'Profit on SDI'
                ELSE NULL
            END AS "ProfitEnhancers"
        FROM
            add_metric_calcs
    )
SELECT 
    *
FROM 
    add_descriptions

-- SELECT job, SUM("NegativeCTCValue"), SUM("POVarianceOppValue"), SUM("ProfitAtRiskValue") 
-- FROM add_metric_calcs 
-- where job = '123040.' and "cost_month" = '2025-11-01' 
-- GROUP BY job
