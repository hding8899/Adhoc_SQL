drop table risk.test.rta_simu;

create temporary table risk.test.risk_score_simu as(

	select  
	rae.auth_event_id
	,rae.user_id
	,rae.auth_event_created_ts
	
	,rae.account_status_cd
	,rae.card_status_cd as card_status_at_txn
	,rae.available_funds

	,rae.card_network_cd
	,rae.card_sub_network_cd
	,rae.auth_event_merchant_name_raw
	,rae.req_amt
	,rae.final_amt
	,rae.trans_ts
	,rae.mti_cd

	,rae.merch_id
	,rae.mcc_cd
	
	,rae.risk_score,rae.pin_result_cd
	,rae.is_international
	,rae.acq_id

	,rae.entry_type
	,rae.is_card_present
	,rae.is_cardholder_present
	--,rae.pan,right(rae.pan,4) as pan_l4d
	--,dual.auth_id as dual_auth_id
	,o2.type as dfe_rule_disable_status
	,o2.timestamp as dfe_rule_disable_time
	,case when rae.card_network_cd='Mastercard' then trim(substr(rae.auth_event_merchant_name_raw,38))
		  when rae.card_network_cd='Visa' then trim(substr(rae.auth_event_merchant_name_raw,37,2))
	 else null end as merchant_state

	,dt.dispute_created_at
	,dt.resolution_decision
	,case when dt.unique_transaction_id is not null then 1 else 0 end as dispute_ind
	,case when dt.unique_transaction_id is not null then datediff(day,rae.auth_event_created_ts,dt.dispute_created_at) else null end as dispute_txn_daydiff
	,case when dt.unique_transaction_id is not null and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then 1 else 0 end as dispute_unauth_ind
	,case when dt.unique_transaction_id is not null and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') and (dt.resolution = 'Pending Resolution' or dt.resolution is null) then 1 else 0 end as dispute_unauth_pending_ind
	,case when dt.resolution_decision in ('approve','Approved') then 1 else 0 end as dispute_aprv_ind
	,case when dt.resolution_decision in ('approve','Approved') and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then 1 else 0 end as dispute_unauth_aprv_ind

	from edw_db.core.fct_realtime_auth_event as rae
	left join edw_db.core.fct_realtime_auth_event as dual on rae.user_id=dual.user_id and (rae.auth_id=dual.original_auth_id)
	left join segment.chime_prod.member_overrides as o2 (on o2.user_id=rae.user_id and o2.type='disable_fraud_rules' and rae.auth_event_created_ts<=dateadd('hour',1,o2.timestamp) and rae.auth_event_created_ts>o2.timestamp)
	left join risk.prod.disputed_transactions as dt on (dt.user_id=rae.user_id and (dt.authorization_code=rae.auth_id or dt.authorization_code=dual.auth_id))

	where 1=1
	and rae.auth_event_created_ts::date between '2022-05-01' and '2022-08-31'
	--and rae.original_auth_id=0
	and rae.entry_type like '%Contactless%'
	and rae.response_cd in ('00','10') /*approved txn*/
	and rae.req_amt<0 /*debit spending only*/
	and 
	qualify row_number() over (partition by rae.auth_event_id order by dual.original_auth_id desc,o2.timestamp,dt.dispute_created_at)=1

);

select count(*),count(distinct auth_event_id) from risk.test.rta_simu
;