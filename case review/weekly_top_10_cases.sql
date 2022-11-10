select
dt.user_id,
date_trunc('week',dt.dispute_created_at) as week,
sum(dt.transaction_amount)*-1 as sum_dispute,
count(*)
from
(
select
dt.*,
d.id as r_inspector_dispute_id,
(case when dt.transaction_code like 'AD%' then 4
              when dt.transaction_code like 'FE%' then 5
              when dt.transaction_code like 'IS%' then 6
              when dt.transaction_code like 'PM%' then 7
              when dt.transaction_code like 'SD%' then 8
              when dt.transaction_code like 'VS%' then 9 else 0 end) transaction_id,
datediff(day, dt.TRANSACTION_TIMESTAMP, dt.dispute_created_at) as days_to_dispute
from risk.prod.disputed_transactions dt
left join fivetran.inspector_public.disputes d on dt.user_dispute_claim_id::varchar = d.claim_ext_id::varchar
where 1=1
and date_trunc('week',dispute_created_at)=dateadd(day, -7, date_trunc('week',current_date))
and transaction_code in ('ISA','ISC','ISJ','ISL','ISM','ISR','ISZ','MPM','MPR','MPW','PLA','PLJ','PLM','PLR','PLW','PLZ','PRA','PRW','SDA','SDC','SDL','SDM','SDR','SDV','SDW','SDZ','VSA','VSC','VSJ','VSL','VSM','VSR','VSW','VSZ')
--sometimes the same transaction might be disputed multiple times, we only want to keep the latest one
qualify row_number() over (partition by dt.UNIQUE_TRANSACTION_ID order by dt.DISPUTE_CREATED_AT desc)=1  
)as dt

left join fivetran.inspector_public.transactions t on dt.r_inspector_dispute_id = t.dispute_id
and to_number(concat(dt.transaction_id, dt.authorization_code)) = to_number(t.transaction_ext_id)

where 1=1
and coalesce(t.investigation_resolution,dt.resolution_decision) in ('approve','Approved')
and reason IN ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer')
group by 1,2
order by sum_dispute desc
limit 10
;
