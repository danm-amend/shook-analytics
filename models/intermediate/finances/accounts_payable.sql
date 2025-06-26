select 
    trim(a.APCo) as APCo, 
    trim(a.Mth) as Mth, 
    trim(a.APTrans) as APTrans, 
    trim(a.APLine) as APLine,
    Trim(a.LineType) as line_type,
    trim(a.JCCo) as JCCo,
    trim(a.Job) as Job, 
    trim(a.PhaseGroup) as phase_group,
    trim(a.Phase) as Phase,
    trim(a.JCCType) as cost_type,
    a.GrossAmt as gross_amount,
    a.TaxAmt as tax_amount,
    a.Retainage as retainage,
    b.PaidDate as paid_date,
    b.DueDate as due_date,
    b.Amount as amount
from {{ source('shookdw', 'APTL') }} as a 
left join 
{{ source('shookdw', 'APTD') }} as b 
-- on a.APCo = b.APCo and a.Mth = b.Mth and a.APTrans = b.APTrans and a.APLine = b.APLine
using(APCo, Mth, APTrans, APLine)
