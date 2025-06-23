 with gl_account_summary as (
    select 
        trim(glco) as gl_co
        , trim(glacct) as gl_account
        , trim(mth) as mth 
        , trim(jrnl) as jrnl
        , trim(glref) as gl_ref
        , trim(sourceco) as source_co
        , trim(source) as source 
        , trim(netamt) as net_amt
        , trim(adjust) as adjust 
        , trim(purge) as purge
        , load_datetime 
    from 
        {{ source('shookdw', 'GLAS') }}
), add_accounts as (
    select 
        gas.*
        , ac.part1 as         natural_account
        , ac.part2 as         account_division
        , ac.part3 as         account_department
        , ac.Description as   account_description
        , ac.AcctType as      account_type
    from 
        gl_account_summary as gas 
        left join 
        {{ source('shookdw', 'GLAC') }} as ac 
        on gas.gl_co = trim(ac.glco) and gas.gl_account = trim(ac.glacct)
), add_division as (
    select 
        aa.*
        , coalesce(dm.Description, glpi.Description) as division_description 
    from add_accounts aa 
    left join 
    {{ source('shookdw', 'bjcdm') }} as dm 
    on trim(aa.account_division) = trim(dm.department) and trim(aa.gl_co) = trim(dm.GlCo)
    left join 
    {{ source('shookdw', 'GLPI') }} as glpi 
    on trim(aa.account_division) = trim(glpi.instance) and aa.gl_co = glpi.GlCo 
    and glpi.PartNo = 2
), add_natural_account_desc as (
    select 
        ad.*,
        glpi.Description as natural_account_desc
    from 
        add_division as ad 
    left join 
        {{ source('shookdw', 'GLPI') }} as glpi
    on ad.natural_account = trim(glpi.instance) and ad.gl_co = trim(glpi.GlCo)
    where glpi.PartNo = 1
), add_department as (
    select 
        na.*,
        glpi.Description as department_description
    from 
        add_natural_account_desc as na 
    left join 
        {{ source('shookdw', 'GLPI') }} as glpi 
    on na.gl_co = trim(glpi.GLCo) and na.account_department = trim(glpi.instance)
    where glpi.PartNo = 3
)

select 
    gl_co,
    gl_account, 
    natural_account,
    natural_account_desc,
    account_division,
    division_description,
    account_department,
    department_description,
    account_description,
    account_type,
    jrnl,
    gl_ref,
    source_co,
    source,
    mth,
    net_amt as net_amount
from add_department
