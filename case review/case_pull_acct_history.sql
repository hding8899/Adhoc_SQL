with user_info as
/*define target user*/
(select distinct to_char(id) as user_id from CHIME.FINANCE.members where id=33118244)


/*pulling FTP txn: transfer deposite credit_adj etc*/
select
t.user_id,
convert_timezone('America/Los_Angeles',t.transaction_timestamp) as timestamp,
t.authorization_code::varchar as id,
case when TRANSACTION_DETAILS_2<>'' then TRANSACTION_DETAILS_2
     when DEPOSIT_TYPE_CD='Cash Deposit' then DEPOSIT_TYPE_CD
     when t.description='Money sent via Pay Friends' then concat('receiver id:',pf.receiver_id)
     when t.description='Money recieved via Pay Friends' then concat('sender id:',pf.sender_id)
     else t.merchant_name end as merchant_name,
t.type,
concat(case when t.description='Money sent via Pay Friends' and txn_type='PF' then 'Money sent via Pay Friends'
     when t.description='Money sent via Pay Friends' and txn_type='PA' then 'Money sent via Pay Anyone'
     when t.description='Money recieved via Pay Friends' and txn_type='PF' then 'Money recieved via Pay Friends'
     when t.description='Money recieved via Pay Friends' and txn_type='PA' then 'Money recieved via Pay Anyone'
     else t.description end,'; Tran_cd: ',transaction_cd,'; Prism Score: ', prs.score) as description,
case when t.unique_program_id IN (512,609,660,2247,2457) and t.card_id<>0 then 'checking '||right(dc.CARD_NUMBER,4)
    when t.unique_program_id IN (600,278,1014,2248,2458) then 'CB'
    when t.unique_program_id IN (512,609,660,2247,2457) and t.card_id=0 then 'savings'
    else t.unique_program_id::varchar end as card_type,
case when t.transaction_cd is not null then 'Approved' end as decision,
'n/a' as decline_resp_cd,
'n/a' as vrs,
'n/a' rules_denied,
t.settled_amt as amt,
case when d.authorization_code is not null then 'yes' else 'no' end as is_disputed
-- ,t.MCC_CD
from edw_db.core.ftr_transaction t
left join (select id,sender_id,receiver_id,memo,abs(amount) as amount,type_code,
          date_trunc('minute',convert_timezone('America/Los_Angeles',created_at)) as minute,
          case when type_code = 'to_member' then 'PF' else 'PA' end as txn_type,
          case when funding_source_type = 'linked_card' then 'Instant' else 'Regular' end as funding
          from MYSQL_DB.CHIME_PROD.PAY_FRIENDS) as pf
  on (t.user_id=pf.sender_id or t.user_id=pf.receiver_id)
  and date_trunc('minute',t.transaction_timestamp)=pf.minute
  and abs(t.settled_amt)=abs(pf.amount)
left join risk.prod.disputed_transactions d
  on t.authorization_code=d.authorization_code
  and t.user_id=d.user_id
left join EDW_DB.CORE.DIM_CARD as dc
on to_char(t.CARD_ID)=dc.CARD_ID and dc.USER_ID=t.USER_ID

left join (select pay_friend_id, score from ml.model_inference.prism_alerts_v2) prs on (prs.pay_friend_id = pf.ID)
where 1=1
and description NOT IN ('Savings Round-Up Transfer','Savings Round-Up Transfer from Checking','API Cardholder Balance Adjustments','Savings Round-Up Transfer Bonus',
'International Cash Withdrawal Fee', 'Domestic Cash Withdrawal Fee - ATM','Savings Interest', 'Payment from the Secured Credit Funding Account to the Secured Credit Card')
and type<>'Purchase' /*non purchase only: fee deposit transfer credit adj etc*/
and MCC_CD not in ('6010','6011') /*Financial Institutions ??? Manual/auto Cash Disbursements*/
and t.user_id IN (select * from user_info)

UNION ALL


/*pull all auth history*/

select
rta.user_id,
convert_timezone('America/Los_Angeles',rta.trans_ts) as timestamp,
rta.auth_id::varchar as id,
rta.auth_event_merchant_name_raw as merchant_name,
case when rta.MCC_CD in ('6010','6011') then 'Withdrawal'
    else 'Purchase' end as type,
concat('Card Transaction ',rta.entry_type, '; MCC_CD: ', rta.mcc_cd) as description, /*EMV, magnetic, contactless etc.*/
dc.CARD_TYPE||' '||right(rta.PAN,4) as card_type,
case when rta.response_cd IN ('00','10') then 'Approved' else 'Declined' end as decision,
case when rta.response_cd IN ('00','10') then 'n/a' else concat(rta.response_cd,' '
                                                                , case when rta.response_cd='51' then 'NSF'
                                                                       when rta.response_cd='04' then 'Inactive Card'
                                                                       when rta.response_cd='05' then 'Do Not Honor'
                                                                       when rta.response_cd='30' then 'Format Error'
                                                                       when rta.response_cd='41' then 'Lost/Stolen'
                                                                       when rta.response_cd='43' then 'Lost/Stolen'
                                                                       when rta.response_cd='54' then 'Mismatched Expry'
                                                                       when rta.response_cd='55' then 'Incorrect PIN'
                                                                       when rta.response_cd='57' then 'Card Disabled'
                                                                       when rta.response_cd='59' then 'DFE'
                                                                       when rta.response_cd='61' then 'Exceeds Limit'
                                                                       when rta.response_cd='75' then 'PIN Tries Exceeded'
                                                                       when rta.response_cd='N7' then 'Incorrect CVV'
                                                                       when rta.response_cd='01' then 'Processor Error'
                                                                       when rta.response_cd='85' then 'Address validation authorization'
                                                                       else rta.response_cd end ) ::varchar end as decline_resp_cd,
rta.risk_score::varchar as vrs,
case when rta.response_cd in ('59') then rta2.policy_name||' -'||(case when o.decision_id is null then rta2.decision_outcome
                                                                       when o.is_suppressed=true then 'suppressed'
                                                                       when o.response_signal is null then 'no response'
                                                                  else o.response_signal end)
     when rta.response_cd in ('00','10') then rta2.policy_name||' -'||rta2.decision_outcome
     else 'n/a' end as rules_denied,
rta.req_amt as amt,
case when d.authorization_code is not null then 'yes' else 'no' end as is_disputed

from edw_db.core.fct_realtime_auth_event rta
left join EDW_DB.CORE.DIM_CARD as dc on rta.USER_ID=dc.USER_ID  and right(rta.PAN,4)=right(dc.CARD_NUMBER,4)
left join edw_db.core.fct_realtime_auth_event dual_auth_settlment on rta.auth_id=dual_auth_settlment.original_auth_id and rta.user_id=dual_auth_settlment.user_id 
left join risk.prod.disputed_transactions d on (d.authorization_code=rta.auth_id or d.authorization_code=dual_auth_settlment.auth_id) and d.user_id=rta.user_id
left join chime.decision_platform.real_time_auth rta2 on 
    (rta.user_id=rta2.user_id and rta.auth_id=rta2.auth_id and rta2.is_shadow_mode='false' and policy_result='criteria_met' and policy_actions like '%'||decision_outcome||'%' and decision_outcome in ('hard_block','merchant_block','deny','prompt_override','sanction_block','allow')) /*2021.11.10 - present*/
left join chime.decision_platform.fraud_override_service o on (rta.user_id=o.user_id and rta.auth_id=o.realtime_auth_id)

where 1=1
and rta.original_auth_id=0 
and rta.final_amt>=0
and rta.user_id IN (select * from user_info)
qualify row_number() over(partition by rta.auth_event_id order by o.response_received_at) = 1


UNION ALL


/*pull false posted txn*/
select events.user_id,
convert_timezone('America/Los_Angeles',events.tran_timestamp) as timestamp,
events.auth_id::varchar as id,
events.merch_name,
'Purchase'  as type,
concat('Card Transaction ','likely force post') as description,
case when events.prog_id IN (512,609,660,2247,2457) then 'checking '||right(rta.PAN,4)
    when events.prog_id IN (600,278,1014,2248,2458) then 'CB '||right(rta.PAN,4)
    else events.prog_id::varchar end as card_type,
'Approved' as decision,
'n/a' as decline_resp_cd,
'n/a' as vrs,
'n/a' rules_denied,
events.amount as amt,
case when d.authorization_code is not null then 'yes' else 'no' end as is_disputed
from mysql_db.chime_prod.alert_authorization_events events
left join edw_db.core.fct_realtime_auth_event rta
  on events.auth_id::varchar=rta.auth_id::varchar
  and events.user_id=rta.user_id
left join risk.prod.disputed_transactions d
  on events.auth_id=d.authorization_code
  and events.user_id=d.user_id
where 1=1
and rta.auth_id is null
and events.type='settle'
and events.user_id IN (select * from user_info)

UNION ALL

/*pull disputed txn*/

select
user_id,
convert_timezone('America/Los_Angeles',dispute_created_at) as timestamp,
user_dispute_claim_txn_id::varchar as id,
-- AUTHORIZATION_CODE,
merchant_name,
'dispute'as type,
reason||'; dispute_claim_id:'||user_dispute_claim_id as description,
INTAKE_TYPE as card_type,
coalesce(investigation_resolution,resolution_decision) as decision,
'n/a' as decline_resp_cd,
'n/a' as vrs,
'n/a' rules_denied,
transaction_amount as amt,
'n/a' as is_disputed
-- from risk.prod.disputed_transactions
from
(
select dt.*
,t.investigation_resolution
from
(
select
dt.*,
d.id as r_inspector_dispute_id,
 case
    when coalesce(dc.intake_type,d.intake_type) = 'mobile' then 'in_app'
    when d.intake_type in ('fax','paper_mail') then 'email'
    when d.intake_type is not null then d.intake_type
    when mc.interactions_str_seq ilike '%app%' then 'chat_bot'
    when mc.contact_flow ilike '%phone%' then 'phone'
    when mc.contact_flow ilike '%email%' then 'email'
    else 'agent_channel_unknown'
    end as intake_type,
(case when dt.transaction_code like 'AD%' then 4
              when dt.transaction_code like 'FE%' then 5
              when dt.transaction_code like 'IS%' then 6
              when dt.transaction_code like 'PM%' then 7
              when dt.transaction_code like 'SD%' then 8
              when dt.transaction_code like 'VS%' then 9 else 0 end) transaction_id,
datediff(day, dt.TRANSACTION_TIMESTAMP, dt.dispute_created_at) as days_to_dispute
from risk.prod.disputed_transactions dt
left join fivetran.inspector_public.disputes d on dt.user_dispute_claim_id::varchar = d.claim_ext_id::varchar
left join fivetran.mysql_rds_disputes.user_dispute_claims dc
on dc.id = dt.user_dispute_claim_id
left join analytics.test.dispute_member_contacts mc
on mc.dispute_id = dt.user_dispute_claim_id
and mc.dispute_contact_category = 'Filing'
left join analytics.test.blocked_self_service_disputes bm
on bm.user_dispute_claim_id = dt.user_dispute_claim_id
)as dt

left join fivetran.inspector_public.transactions t on dt.r_inspector_dispute_id = t.dispute_id
and concat(dt.transaction_id, dt.authorization_code) = t.transaction_ext_id

qualify row_number() over (partition by dt.UNIQUE_TRANSACTION_ID order by dt.DISPUTE_CREATED_AT desc)=1
) as disputes
where 1=1
and user_id IN (select * from user_info)

UNION ALL

/*pull all logins success:
  If indicated in description column:
    SMS 2FA Auth - passed 2fa auth
    Scan ID Auth - passed scan ID
    Step Down - passed password(and arkose) 
  
*/
 select 
    ls.user_id,
    convert_timezone('America/Los_Angeles',ls.session_timestamp) as timestamp,
    ls.device_id::varchar as id,
    'n/a' as merchant_name,
    'login' as type,
     concat(COALESCE(concat('ATOMv2 score:',atomv2.score),''),'  ',COALESCE(concat('DEVICE:',ls.device_model),''),'  ',coalesce(concat('LOCALE:',ls.locale),''),'  ',coalesce(concat('TZ:',ls.timezone),''),'',coalesce(concat('CARRIER:',ls.network_carrier),''),'  ',coalesce(concat('IP:',ls.ip,' ',loc.time_zone),''),' ',COALESCE(concat('Platform:',ls.platform),''),' '
    ,coalesce(concat('TFA_METHOD:',lr.tfa_method_deprecated),''),' ', coalesce('TIME_SPENT:'||cast(datediff(second,login_started_at,login_success_at) as varchar),'')) as description,
    'n/a' as card_type,
    'n/a' as decision,
    'n/a' as decline_resp_cd,
    'n/a' as vrs,
    'n/a' rules_denied,
    0 as amt,
    'n/a' as is_disputed
    from edw_db.feature_store.atom_user_sessions_v2 ls
    left join partner_db.maxmind.ip_geolocation_mapping as map on ls.ip=map.ip
    left join partner_db.maxmind.GEOIP2_CITY_LOCATIONS_EN loc on loc.geoname_id = map.geoname_id
    left join ml.model_inference.ato_login_alerts atomv2 on ls.user_id=atomv2.user_id and ls.device_id=atomv2.device_id and atomv2.score<>0
    left join analytics.test.login_requests lr on (lr.user_id=ls.user_id and lr.segment_device_id=ls.device_id and lr.login_success_at between dateadd(second, -60, ls.session_timestamp) and ls.session_timestamp)
   where 1=1
   and ls.user_id IN (select * from user_info)
   
   qualify row_number() over (partition by ls.session_timestamp, ls.device_id order by lr.login_success_at desc)=1 

UNION ALL

/*Failed logins*/
select user_id,
       convert_timezone('America/Los_Angeles',login_started_at)::timestamp as timestamp,
       segment_device_id as id,
       'n/a' as merchant_name,
       'login failed' as type,
       'FALIED RSN: '||tfa_method_deprecated||'; PLATFORM: '||platform as description,
       'n/a' as card_type,
       'n/a' as decision,
       'n/a' as declined_resp_cd,
       'n/a' as vrs,
       'n/a' as rule_denied,
        0 as amt,
       'n/a' as is_disputed    
    from analytics.test.login_requests
    where 1=1
    and user_id IN (select * from user_info)
    and tfa_method is not null /*exclude pre login fail*/
    and login_success=0 and mfa_auth_success=0 /*exclude data issue casued fail*/

UNION ALL

/*info chg*/
select
item_id as user_id,
convert_timezone('America/Los_Angeles',created_at)::timestamp as timestamp,
versions_id::varchar as id,
'n/a' as merchant_name,
case when item_change='status' then 'status change' else concat(item_change,' ','change') end as type,
concat(concat('change from:',change_from),' ',concat('to:',change_to),' ',concat('by: ',WHODUNNIT)) as description,
'n/a' as card_type,
'n/a' as decision,
'n/a' as decline_resp_cd,
'n/a' as vrs,
'n/a' rules_denied,
0 as amt,
'n/a' as is_disputed
from
  (select * , split_part(split_part(object_changes,':',1),'---',2) as item_change
  from analytics.looker.versions_pivot
  where 1=1
  and ( item_change ilike '%zip_code%'
      or  item_change ilike '%status%'
      or  item_change ilike '%state_code%'
      or  item_change ilike '%phone%'
      or  item_change ilike '%last_name%'
      or  item_change ilike '%first_name%'
      or  item_change ilike '%email%'
      or  item_change ilike '%address%')
      ) pii
where 1=1
and item_type = 'User'
and item_id IN (select * from user_info)

UNION ALL

/*app view activity*/
select
try_to_number(user_id) as user_id,
convert_timezone('America/Los_Angeles',original_timestamp)::timestamp as timestamp,
context_device_id::varchar as id,
'n/a' as merchant_name,
'app_view_activity' as type,
concat(concat('app location: ',location),' ',concat('; label: ',label),' ',concat('; what was viewed: ',unique_id)) as description,
'n/a' as card_type,
'n/a' as decision,
'n/a' as decline_resp_cd,
'n/a' as vrs,
'n/a' rules_denied,
0 as amt,
'n/a' as is_disputed
from segment.chime_prod.menu_button_tapped
where 1=1
and (unique_id ilike '%account%' or  unique_id ilike '%card%')
and location<>'Dialogue'
and try_to_number(user_id) IN (select * from user_info)

union all

/*card replacement records*/
select *
from (
select
USER_ID,
lead(CARD_CREATED_TS) over (partition by CARD_TYPE order by CARD_CREATED_TS) as timestamp,
CARD_NUMBER||' to '||
    lead(CARD_NUMBER) over (partition by CARD_TYPE order by CARD_CREATED_TS) as id,
'n/a' as merchant_name,
'card_replacement' as type,
'old card status changed to '||CARD_STATUS||' on '||LAST_STATUS_CHANGE_DT
    as description,
card_type as card_type,
'n/a' as decision,
'n/a' as decline_resp_cd,
'n/a' as vrs,
'n/a' rules_denied,
0 as amt,
'n/a' as is_disputed
from EDW_DB.CORE.DIM_CARD
where 1=1
and user_id IN (select * from user_info)
and SHIPPED_DT is not null
) as card_replacement
where 1=1
and timestamp is not null

order by timestamp
;
