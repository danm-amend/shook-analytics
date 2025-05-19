select 
    a.APCo, 
    a.Mth, 
    a.APTrans, 
    a.APLine,
    a.LineType as line_type,
    a.JCCo,
    trim(a.Job) as Job, 
    a.PhaseGroup as phase_group,
    a.Phase,
    a.JCCType as cost_type,
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
