select 
    trim(ARCo) as ARCo,
    trim(Mth) as Mth,
    trim(ARTrans) as ARTrans,
    trim(ARTransType) as ARTransType,
    trim(CUSTGroup) as CUSTGroup,
    trim(Customer) as Customer,
    trim(CustRef) as CustRef,
    trim(JCCo) as JCCo,
    trim(Contract) as Contract,
    trim(Invoice) as Invoice,
    trim(TransDate) as TransDate,
    trim(AppliedMth) as AppliedMth,
    AppliedTrans,
    Invoiced,
    Paid,
    Retainage,
    DiscTaken,
    AmountDue,
    PayFullDate
from {{ source('shookdw', 'ARTH') }}
