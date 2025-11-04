with grading_metrics as (
    select 
        cast(a.proj_id as string) as proj_id,
        a.proj_name, 
        a.num_hard_consts,
        a.hard_consts_grade,
        a.pct_soft_consts,
        a.sof_consts_grade,
        a.num_invalid_dates,
        a.invalid_date_grade,
        b.pct_high_float,
        b.high_float_grade,
        b.total_float_days,
        b.negative_float_grade,
        b.TFCI,
        b.tfci_grade,
        b.cpli,
        b.cpli_grade,
        c.missing_logic_pct,
        c.missing_logic_grade,
        d.fs_pct,
        d.fs_grade,
        e.lags_pct,
        e.lags_grade,
        e.leads_pct,
        e.leads_grade,
        f.out_seq_pct,
        f.out_seq_grade,
        g.resource_look_ahead_pct,
        g.resource_look_ahead_grade,
        h.bei,
        h.bei_grade,
        h.missed_pct,
        h.miss_act_grade,
        i.long_dur_pct,
        i.dur_grade
    from 
        {{ ref('constraints') }} as a 
    left join 
        {{ ref('float') }} as b 
        using(proj_id)
    left join 
        {{ ref('missing_logic') }} as c 
        using(proj_id)
    left join 
        {{ ref('relationships') }} as d
        using(proj_id)
    left join 
        {{ ref('lag_lead') }} as e
        using(proj_id)
    left join 
        {{ ref('out_of_sequence') }} as f
        using(proj_id)
    left join 
        {{ ref('resource_look_ahead') }} as g
        using(proj_id)
    left join 
        {{ ref('baseline_metrics') }} as h
        using(proj_id)
    left join 
        {{ ref('high_duration') }} as i
        using(proj_id)
), long_format as (
    select proj_id, proj_name, 'hard_constraints' as metric_name, num_hard_consts as metric_value, hard_consts_grade as metric_grade
    from grading_metrics

    union all select proj_id, proj_name, 'soft_constraints', pct_soft_consts, sof_consts_grade
    from grading_metrics

    union all select proj_id, proj_name, 'invalid_dates', num_invalid_dates, invalid_date_grade
    from grading_metrics

    union all select proj_id, proj_name, 'high_float', pct_high_float, high_float_grade
    from grading_metrics

    union all select proj_id, proj_name, 'total_float_days', total_float_days, negative_float_grade
    from grading_metrics

    union all select proj_id, proj_name, 'tfci', TFCI, tfci_grade
    from grading_metrics

    union all select proj_id, proj_name, 'cpli', cpli, cpli_grade
    from grading_metrics
    
    union all select proj_id, proj_name, 'missing_logic', missing_logic_pct, missing_logic_grade
    from grading_metrics
    
    union all select proj_id, proj_name, 'fs_relationships', fs_pct, fs_grade
    from grading_metrics

    union all select proj_id, proj_name, 'lags', lags_pct, lags_grade
    from grading_metrics

    union all select proj_id, proj_name, 'leads', leads_pct, leads_grade
    from grading_metrics
    
    union all select proj_id, proj_name, 'out_of_sequence', out_seq_pct, out_seq_grade
    from grading_metrics

    union all select proj_id, proj_name, 'resource_look_ahead', resource_look_ahead_pct, resource_look_ahead_grade
    from grading_metrics

    union all select proj_id, proj_name, 'bei', bei, bei_grade
    from grading_metrics
    
    union all select proj_id, proj_name, 'missed_activities', missed_pct, miss_act_grade
    from grading_metrics
    
    union all select proj_id, proj_name, 'long_duration', long_dur_pct, dur_grade
    from grading_metrics
)

select
    *
    ,CASE
        WHEN metric_name = 'hard_constraints' THEN 'Hard Constraints (#)'
        WHEN metric_name = 'soft_constraints' THEN 'Soft Constraints (%)'
        WHEN metric_name = 'invalid_dates' THEN 'Invalid Dates (#)'
        WHEN metric_name = 'high_float' THEN 'High Float (#)'
        WHEN metric_name = 'total_float_days' THEN 'Total Float Days (#)'
        WHEN metric_name = 'tfci' THEN 'Total Float Consumption Index (TFCI)'
        WHEN metric_name = 'cpli' THEN 'Critical Path Length Index (CPLI)'
        WHEN metric_name = 'missing_logic' THEN 'Missing Logic (#)'
        WHEN metric_name = 'fs_relationships' THEN 'FS Relationship (%)'
        WHEN metric_name = 'lags' THEN 'Lags (%)'
        WHEN metric_name = 'leads' THEN 'Leads (%)'
        WHEN metric_name = 'out_of_sequence' THEN 'Out of Sequence Activities'
        WHEN metric_name = 'resource_look_ahead' THEN 'Resource Lookahead'
        WHEN metric_name = 'bei' THEN 'Baseline Execution Index (BEI)'
        WHEN metric_name = 'missed_activities' THEN 'Missed Activities (%)'
        WHEN metric_name = 'long_duration' THEN 'Long Duration'
        ELSE NULL
    END AS metric_full_name
    ,CASE
        WHEN metric_grade = 'A' THEN 4
        WHEN metric_grade = 'B' THEN 3
        WHEN metric_grade = 'C' THEN 2
        WHEN metric_grade = 'D' THEN 1
        WHEN metric_grade = 'F' THEN 0
        ELSE NULL
    END AS grade_points
from long_format
