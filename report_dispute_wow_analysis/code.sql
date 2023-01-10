/*

This code is going to pull the focal entity week(credit and debit purchase settled auth with 7day dispute ind) + the previous week to do:
    1. total weekly disputed $ change explanation
    2. analyze segments with high dispute rate for potential policy oppotunities

Tables:
    1. latest 7d matured txn weeks settled auth with 7d dispute ind, transaction cd, close code, resolution outcome etc.
    2. aggregated view with total txn and disputed $ for dispute rate comparison per segment

Note:
    -- looker dispute trend
    https://chime.looker.com/dashboards/1542?Txn+Mth+Month=12+month&Transaction+Type=Debit+Purchase&Txn+Week=48+week+ago+for+48+week

    -- dispute trend underlying code
    https://github.com/1debit/data/tree/main/database/snowflake/risk/prod/ddl/views

    -- hex dashboard
    https://chime.hex.tech/global/hex/bf84ed59-6279-4f26-b7f3-9a6398fe1117/draft/logic
    
    -- the settled amount for credit/debit purchase could still be negative due to the inclusion of refund txn code like VSZ in the original data pulling code

*/

/*>>>>>> stored procedure creation*/
create or replace procedure risk.test.hding_dispute_wow_analysis_sp(day1 date, day2 date, dispute_win integer)
returns varchar
language sql
as

$$
begin


create or replace table risk.test.hding_7d_dispute_wow as(
    with t1 as(
        select 
        a.user_id
        ,a.authorization_code
        ,a.transaction_timestamp
        ,m.state_cd as customer_state_cd
        ,trim(substr(a.merchant_name_raw,37,2)) as merchant_state_cd
        ,customer_state_cd<>merchant_state_cd as cust_mrch_state_diff
        ,date_trunc('week', a.transaction_timestamp)::date as txn_week    
        ,case when a.transaction_cd in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW','SDW') then 'atm'
              when a.unique_program_id in (600, 278, 1014, 2248) then 'credit_purchase' 
         else 'debit_purchase' end as card_type
        ,datediff(month,m.enrollment_initiated_ts,a.transaction_timestamp) as mondiff_enrollinit_auth
        ,case when mondiff_enrollinit_auth<12 then '1. <12'
              when mondiff_enrollinit_auth<24 then '2. 12-24'
              when mondiff_enrollinit_auth<36 then '3. 24-36'
              else '9. >=36' end as mob_cat
        ,a.merchant_name_raw
        ,a.transaction_cd
        ,a.unique_program_id
        ,a.settled_amt*-1 as settled_amt
        ,case when a.settled_amt*-1<=0 then '1. <=0'
              when a.settled_amt*-1<100 then '2. 0-100'
              when a.settled_amt*-1<500 then '3. 100-500'
              when a.settled_amt*-1<1000 then '4. 500-1k'
              else '9. >=1k' end as settled_amt_cat
        ,a.mcc_cd
        ,rae.risk_score
        ,rae.is_international
        ,rae.card_network_cd
        ,case when rae.risk_score<10 then '1. <10'
              when rae.risk_score<20 then '2. 10-20'
              when rae.risk_score<30 then '3. 20-50'
              when rae.risk_score<40 then '4. 30-40'
              when rae.risk_score<50 then '5. 40-50'
              else '9. >=50' end as risk_score_cat
        ,rae.entry_type
        ,case when d.dispute_created_at<=dateadd(day,:dispute_win,a.transaction_timestamp) then 1 else 0 end as dispute_ind_7d
        ,d.reason
        ,d.resolution_code
        ,d.close_code
        ,coalesce(d.updated_transaction_amount,a.settled_amt*-1) as updated_transaction_amount
            from edw_db.core.fct_settled_transaction a
            left join risk.prod.disputed_transactions d on (a.user_id=d.user_id and a.authorization_code=d.authorization_code)
            left join edw_db.core.fct_realtime_auth_event rae on (a.user_id=rae.user_id and rae.auth_id=a.authorization_code)
            left join edw_db.core.dim_member m on (a.user_id=m.user_id)
            where 1=1
            --and date_trunc('week', a.transaction_timestamp)::date in ('2022-11-28','2022-12-19') /*!!!!!!! change per analysis purpose!*/
            and a.transaction_timestamp::date between :day1 and :day2
            and datediff(day,a.transaction_timestamp, current_date())>=:dispute_win /*exclude auth which are not mature per dispute perf windows*/
            and a.transaction_cd in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL',
                                     'VSM', 'VSR', 'VSZ','SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM',
                                     'PLA', 'PRA', 'SSA', 'SSC', 'SSZ' /*!credit or debit purchase only!*/
                                    ,'VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW','SDW' /*!atm!*/
                                    ) 
    ), t2 as(
        /*append top 10 mcc and merchant name based on last txn week*/
        select a.*
            ,case when b.rnk_mcc<=9 then cast(rnk_mcc as varchar(10))||'.'||cast(a.mcc_cd as varchar(10)) else '99.others' end as top_mcc_cd
            ,case when c.rnk_mrch<=20 then cast(rnk_mrch as varchar(10))||'.'||a.merchant_name_raw else '99.others' end as top_merchant_name_raw
            ,case when d.rnk_mrch_state<=9 then cast(rnk_mrch_state as varchar(10))||'.'||a.merchant_state_cd else '99.others' end as top_merchant_state_cd
            from t1 a
            left join 
                (
                select a.*,row_number() over (partition by card_type order by dispute_sum desc) as rnk_mcc
                   from(
                       select card_type,mcc_cd,sum(settled_amt) as dispute_sum
                        from t1
                        where 1=1
                        and dispute_ind_7d=1
                        and txn_week=(select max(txn_week) from t1)
                        group by 1,2
                   ) a
                ) b on (a.mcc_cd=b.mcc_cd and a.card_type=b.card_type)
            left join   
                (
                select a.*,row_number() over (partition by card_type order by dispute_sum desc) as rnk_mrch
                   from(
                       select card_type,merchant_name_raw,sum(settled_amt) as dispute_sum
                        from t1
                        where 1=1
                        and dispute_ind_7d=1
                        and txn_week=(select max(txn_week) from t1)
                        group by 1,2
                   ) a
                ) c on (a.merchant_name_raw=c.merchant_name_raw and a.card_type=c.card_type)
            left join   
                (
                select a.*,row_number() over (partition by card_type order by dispute_sum desc) as rnk_mrch_state
                   from(
                       select card_type,merchant_state_cd,sum(settled_amt) as dispute_sum
                        from t1
                        where 1=1
                        and dispute_ind_7d=1
                        and txn_week=(select max(txn_week) from t1)
                        group by 1,2
                   ) a
                ) d on (a.merchant_state_cd=d.merchant_state_cd and a.card_type=d.card_type)

    ),t3 as(
        /*append past 7 day new dvc login score if any*/
        select a.user_id, a.authorization_code, max(b.score) as score
            from t2 a
            left join ml.model_inference.ato_login_alerts b on (a.user_id=b.user_id and b.session_timestamp between dateadd(day,-7,a.transaction_timestamp) and a.transaction_timestamp and b.score>0)
            where 1=1
            group by 1,2
    
    )
    select a.*
    , case when b.score is not null then 'new dvc login' else 'no new dvc' end as new_dvc_login_ind
    , case when b.score is null then '0. no new dvc'
           when b.score<0.1 then '1. <0.1'
           when b.score<0.2 then '2. <0.2'
           when b.score<0.3 then '3. <0.3'
           when b.score<0.4 then '4. <0.4'
           when b.score<0.5 then '5. <0.5'
           else '9. >=0.5' end as atom_score_cat
    from t2 a
    left join t3 b on (a.user_id=b.user_id and a.authorization_code=b.authorization_code)
        
);


create or replace table risk.test.hding_7d_dispute_wow_summarized as(

    select 
        date_trunc('week', a.transaction_timestamp)::date as txn_week
        ,card_type
        ,customer_state_cd
        ,top_merchant_state_cd
        ,cust_mrch_state_diff
        ,mob_cat
        ,card_network_cd
        ,new_dvc_login_ind
        ,atom_score_cat
        ,top_mcc_cd
        ,top_merchant_name_raw
        ,transaction_cd
        ,unique_program_id
        ,settled_amt_cat
        ,risk_score_cat
        ,entry_type
        ,is_international
        ,sum(settled_amt) as settled_amt_sum
        ,sum(case when dispute_ind_7d=1 then settled_amt else 0 end)  as dispute_amt_sum

    from  risk.test.hding_7d_dispute_wow a
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
    
);

end;
$$
;



/*>>>>> task creation*/
create or replace task risk.test.hding_dispute_wow_analysis 
    warehouse=risk_wh
    schedule='using cron 0 6 * * MON America/Los_Angeles'
as
  call risk.test.hding_dispute_wow_analysis_sp(date_trunc('week', dateadd(day,-7*5,current_timestamp()))::date
                                              ,dateadd(day,-1,date_trunc('week', dateadd(day,-7*1,current_timestamp()))::date)
                                              ,7);

/*>>>>> more task related operation*/
show tasks like 'hding%' in risk.test;/*show tasks under hding*/
alter task  risk.test.hding_dispute_wow_analysis resume; /*turn it on*/
/*for more detailed schedule and next run ts*/
select *
  from table(analytics.information_schema.task_history(
      TASK_NAME=>'hding_dispute_wow_analysis'
  ))
-- where database_name='RISK'
-- where state='SUCCEEDED'
  order by scheduled_time desc;


