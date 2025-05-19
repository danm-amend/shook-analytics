WITH 
    job_bounds AS (
      SELECT 
        "Job"
        ,MIN(DATE_TRUNC('MONTH', CAST("Mth" AS DATE))) AS "min_month"
        ,MAX(DATE_TRUNC('MONTH', CAST("Mth" AS DATE))) AS "max_month"
      FROM {{ source('shookdw', 'bjccd') }}
      GROUP BY "Job"
    ),
    current_job_bounds AS (
        SELECT *
        FROM job_bounds
        WHERE "min_month" >= DATE_TRUNC('MONTH', DATEADD(YEAR, -8, CURRENT_DATE))
    ), 
    month_numbers AS (
      SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS "month_offset"
      FROM TABLE(GENERATOR(ROWCOUNT => 200))  -- enough months to cover any job span
    ),
    job_months AS (
      SELECT 
        cpb."Job"
        ,DATEADD(MONTH, mn."month_offset", cpb."min_month") AS "cost_month"
      FROM current_job_bounds cpb
          JOIN month_numbers mn
            ON mn."month_offset" <= DATEDIFF(MONTH, cpb."min_month", cpb."max_month")
    ),
    job_phases AS (
      SELECT DISTINCT "Job","CostType","PhaseGroup","Phase"
      FROM {{ source('shookdw', 'bjccd') }}
    ),
    job_cost_months AS (
      SELECT 
        jp."Job"
        ,jp."CostType"
        ,jp."PhaseGroup"
        ,jp."Phase"
        ,jm."cost_month"
      FROM job_phases jp
          JOIN job_months jm
            ON jp."Job" = jm."Job"
    ),
    job_cost_agg AS (
        SELECT
            --"PostedDate"
            "Job"
            ,"CostType"
            ,"PhaseGroup"
            ,"Phase"
            ,CAST("Mth" AS DATE) AS "MonthDate"
            ,SUM(CASE WHEN "JCTransType" = 'OE' THEN "EstCost" ELSE 0 END) AS "OriginalEst"
            ,SUM(CASE WHEN "JCTransType" IN ('OE', 'CO') THEN "EstCost" ELSE 0 END) AS "CurrentEst"
            ,SUM(CASE WHEN "JCTransType" = 'PF' THEN "ProjCost" ELSE 0 END) AS "Projected"
            --,SUM("ForecastCost") AS "ForecastCost"
            ,SUM("ActualCost") AS "ActualCost"
            ,SUM("TotalCmtdCost") AS "CommittedCost"
        FROM 
            {{ source('shookdw', 'bjccd') }}
        GROUP BY
            "Job"
            ,"CostType"
            ,"PhaseGroup"
            ,"Phase"
            ,CAST("Mth" AS DATE)
    ),
    jc_months_filled AS (
        SELECT 
            jcm."Job"
            ,jcm."CostType"
            ,jcm."PhaseGroup"
            ,jcm."Phase"
            ,jcm."cost_month"
            ,COALESCE(jca."OriginalEst", 0) AS "OriginalEst"
            ,COALESCE(jca."CurrentEst", 0) "CurrentEst"
            ,COALESCE(jca."Projected", 0) AS "Projected"
            ,COALESCE(jca."ActualCost", 0) AS "ActualCost"
            ,COALESCE(jca."CommittedCost", 0) AS "CommittedCost"
        FROM job_cost_months jcm
          LEFT JOIN job_cost_agg jca
            ON jcm."Job" = jca."Job"
            AND jcm."CostType" = jca."CostType"
            AND jcm."PhaseGroup" = jca."PhaseGroup"
            AND jcm."Phase" = jca."Phase"
            AND jcm."cost_month" = jca."MonthDate"
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
            jmf."Job"
            ,jmf."cost_month"
            ,jmf."CostType" AS "CostTypeNumber"
            ,ct."Description" AS "CostTypeName"
            ,jmf."PhaseGroup"
            ,jmf."Phase"
            ,SUBSTR(jmf."Phase", 1,2) AS "Division"
            ,jp."Description"
            
            ,jmf."OriginalEst"
            ,SUM(jmf."OriginalEst") OVER (
                PARTITION BY jmf."Job", jmf."CostType", jmf."PhaseGroup", jmf."Phase"
                ORDER BY jmf."cost_month"
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeOriginalEst"
            
            ,jmf."CurrentEst"
            ,SUM(jmf."CurrentEst") OVER (
                PARTITION BY jmf."Job", jmf."CostType", jmf."PhaseGroup", jmf."Phase"
                ORDER BY jmf."cost_month"
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeCurrentEst"
            
            ,jmf."Projected"
            ,SUM(jmf."Projected") OVER (
                PARTITION BY jmf."Job", jmf."CostType", jmf."PhaseGroup", jmf."Phase"
                ORDER BY jmf."cost_month"
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeProjected"
            
            ,jmf."ActualCost"
            ,SUM(jmf."ActualCost") OVER (
                PARTITION BY jmf."Job", jmf."CostType", jmf."PhaseGroup", jmf."Phase"
                ORDER BY jmf."cost_month"
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeActualCost"
            
            ,jmf."CommittedCost"
            ,SUM(jmf."CommittedCost") OVER (
                PARTITION BY jmf."Job", jmf."CostType", jmf."PhaseGroup", jmf."Phase"
                ORDER BY jmf."cost_month"
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS "CumulativeCommittedCost"
            
        FROM jc_months_filled AS jmf
            LEFT JOIN {{ source('shookdw', 'bjcjp') }} AS jp
                ON jmf."Job" = jp."Job"
                AND jmf."Phase" = jp."Phase"
            LEFT JOIN {{ source('shookdw', 'bjcct') }} AS ct
                ON jmf."PhaseGroup" = ct."PhaseGroup"
                AND jmf."CostType" = ct."CostType"
            /*INNER JOIN jc_cleanup AS jcc
                ON jmf."Job" = jcc."Job"
                AND jmf."CostType" = jcc."CostType"
                AND jmf."PhaseGroup" = jcc."PhaseGroup"
                AND jmf."Phase" = jcc."Phase"*/
    )
SELECT *
FROM job_cost_detail