--/*
WbExport -type=text
-file='P:\Reserving\04_Model_Setup\Data Model Input Process Scripts\LSM Scripts\LOADFILE_CLM_LDATE_COMMERCIAL_MOTOR_LSM.csv'
-delimiter=','
-decimal='.'
-quoteChar='"'
-dateFormat='yyyy-MM-dd';
--*/

WITH  month_end_claim as
(
select claim_number,
trunc(claim_loss_time) row_date,
loss_cause_description,
line_of_business,
brand_name,
rcp_code,
policy_source_system,
scd_start_date,
scd_end_date,
dap_source_system_name,
policy_effective_date,
policy_inception_date,
policy_expiration_date
FROM dap_lsm_claims_commercial_motor.dim_claim_hist
where retired_flag = 0
and nvl(claim_state,'Draft') != 'Draft'
--and  dap_source_system_name = 'CC9'
and scd_start_date  < date_trunc('month', getdate()) and   scd_end_date >= date_trunc('month', getdate())   -- To bring the records which was active at the last date of the pervious month
and claim_number in
(
'088345756','089298616','089440879','089451587','089585848','090627290','091994335','091997684','702045378','702911231',
'702935685','702968520','702977547','702978032','702978073','702986605','702986639','702986712','702986720','702988775
)
),

month_end_exposure as
(
select claim_number,
exposure_order,
exposure_type,
loss_party_type,
head_of_damage,
scd_start_date,
scd_end_date,
dap_source_system_name
from dap_lsm_claims_commercial_motor.dim_claim_exposure_hist
where retired_flag = 0
and nvl(exposure_state,'Draft') != 'Draft'
--and  dap_source_system_name = 'CC9'
and scd_start_date  < date_trunc('month', getdate()) and   scd_end_date >= date_trunc('month', getdate())
and claim_number in
(
'088345756','089298616','089440879','089451587','089585848','090627290','091994335','091997684','702045378','702911231',
'702935685','702968520','702977547','702978032','702978073','702986605','702986639','702986712','702986720','702988775
)

),

transaction_line_level as (
SELECT fctj.claim_number,
       dch.rcp_code,
       dch.policy_source_system,
       dch.policy_effective_date,
       dch.policy_inception_date,
       dch.policy_expiration_date,
       trunc(date_trunc ('month',dch.policy_effective_date)) AS policy_start_month,
       loss_cal.cal_date AS loss_date,
       trunc(date_trunc ('month',loss_cal.cal_date)) AS row_date,
       datediff (months , trunc(date_trunc ('month',loss_cal.cal_date))  ,  trunc(date_trunc ('month', trans_cal.cal_date )) ) + 1 as dev_period ,
       round (case when  loss_cal.cal_year_no = 1999  THEN 50000  ELSE   (1.07 ^  (loss_cal.cal_year_no - 1999)) * 50000   end ,4) as  threshold,
       fctj.claim_transaction_status_id,
       case when db.brand_code  = 'Direct Line' then 'DL4B'  else db.brand_code end AS brand,
       jda_product_type AS cover_type,
       fctj.dap_source_system_name AS claim_system,
       trans_cal.cal_date AS transaction_date,
       dct.payment_type,
       CASE WHEN dct.payment_type = 'Payment' THEN  jda_base_currency_amount ELSE  0  END as payment_with_recovery,
       CASE WHEN dct.payment_type = 'Reserve' THEN  jda_base_currency_amount ELSE  0  END as reserve_with_recovery,
       CASE WHEN dct.payment_type = 'Recovery'  AND  jda_line_category_code in ( 'salvage_Ins' , 'subrog_Ins' , 'excesscoll_Ins' , 'othercoll_Ins'  )  THEN  jda_base_currency_amount
            WHEN dct.payment_type = 'Recovery'  THEN jda_base_currency_amount * -1
             ELSE  0
       END as recovery,
       CASE WHEN dct.payment_type = 'RecoveryReserve' THEN   jda_base_currency_amount  * -1  ELSE  0  END as recovery_reserve,
       CASE
         WHEN dceh.loss_party_type in ('Third Party liability', 'Third-party liability') AND dceh.exposure_type IN ('Bodily Injury','Medical Expenses') THEN 'BI'
         WHEN dceh.loss_party_type in ('Third Party liability', 'Third-party liability') THEN 'TP'
         WHEN dlc.loss_cause_desc = 'Accident - Glass/Windscreen Damage' THEN 'Windscreen'
         WHEN dlc.loss_cause_desc IN ('Fire - Fire damage to vehicle','Theft - Stolen vehicle','Theft - Theft of Keys Only','Theft - Theft from Vehicle') THEN 'Theft'
         ELSE 'AD'
       END AS peril
FROM dap_lsm_claims_commercial_motor.fact_claims_transaction_journey fctj
  INNER JOIN month_end_claim as dch ON concat(replicate('0', 9-length(fctj.claim_number)),fctj.claim_number) = dch.claim_number
  LEFT JOIN dap_lsm_claims_commercial_motor.dim_claim_exposure_hist as exp_hist ON fctj.claim_exposure_hist_id = exp_hist.claim_exposure_hist_id
  LEFT JOIN month_end_exposure as dceh ON concat(replicate('0', 9-length(fctj.claim_number)),fctj.claim_number) = dceh.claim_number and exp_hist.exposure_order = dceh.exposure_order
  LEFT JOIN dap_lsm_claims_commercial_motor.dim_loss_cause dlc ON dlc.loss_cause_id = fctj.loss_cause_id
  LEFT JOIN dap_lsm_claims_commercial.dim_brand db ON db.brand_id = fctj.brand_id
  LEFT JOIN dap_lsm_claims_commercial_motor.dim_cost_type dct ON dct.cost_type_id = fctj.cost_type_id
  LEFT JOIN dap_lsm_claims_commercial.dim_calendar loss_cal ON loss_cal.date_id = fctj.loss_date_id
  LEFT JOIN dap_lsm_claims_commercial.dim_calendar trans_cal ON trans_cal.date_id = fctj.transaction_date_id
  WHERE claim_transaction_status_id = 1 and jda_transaction_status_cd = 'submitted' and coalesce(jda_line_of_business,'blah') not in ('Legal Protection') and transaction_date <= trunc(date_trunc('month',getdate()) -1) --and fctj.dap_source_system_name = 'CC5'
and fctj.claim_number in
(
'088345756','089298616','089440879','089451587','089585848','090627290','091994335','091997684','702045378','702911231',
'702935685','702968520','702977547','702978032','702978073','702986605','702986639','702986712','702986720','702988775
)

),

--select peril,
--sum(payment_with_recovery) as x
--from transaction_line_level
--where claim_system = 'CC5'
--group by peril
--limit 100;

--group transaction to Day Level
transaction_day_level  AS
(
select
claim_number,
rcp_code,
policy_source_system,
row_date,
dev_period,
threshold,
brand,
cover_type,
claim_system,
transaction_date,
peril,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month,
SUM ( payment_with_recovery + recovery ) as  pay,
SUM ( reserve_with_recovery + recovery_reserve )  as est ,
SUM ( payment_with_recovery + recovery + reserve_with_recovery + recovery_reserve  ) as inc ,
SUM ( payment_with_recovery ) gross_payment,
SUM ( recovery )  as  recovery,
SUM ( reserve_with_recovery ) as gross_reserve ,
SUM ( recovery_reserve ) as recovery_reserve,
MIN ( reserve_with_recovery ) as min_gross_reserve ,
MIN ( recovery_reserve ) as min_recovery_reserve
FROM  transaction_line_level
GROUP BY
claim_number,
rcp_code,
policy_source_system,
row_date,
dev_period,
threshold,
brand,
cover_type,
claim_system,
transaction_date,
peril,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month
),
--Cumulative values at claim , Peril level
transaction_cumulative as
(
select
claim_number,
rcp_code,
policy_source_system,
row_date,
dev_period,
threshold,
brand,
cover_type,
claim_system,
peril,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month,
transaction_date,
pay,
est,
inc,
min_gross_reserve,
min_recovery_reserve,
rank() over (partition by claim_number  , peril order  by transaction_date ) as  txn_seq,
coalesce ( sum( pay ) over ( partition by claim_number , peril order by transaction_date rows  unbounded preceding  ), 0)  as  cumulative_pay ,
coalesce ( sum( est ) over ( partition by claim_number , peril order by transaction_date rows  unbounded preceding   ) , 0)as cumulative_est,
coalesce ( sum( inc ) over ( partition by claim_number , peril order by transaction_date  rows unbounded preceding   ) , 0 ) as cumulative_inc,
coalesce ( sum( gross_reserve ) over  (  partition by claim_number , peril order by transaction_date  rows  unbounded preceding  ) ,0 ) as cumulative_gross_reserve,
coalesce ( sum( recovery_reserve ) over  (  partition by claim_number , peril order by transaction_date  rows  unbounded preceding  ) , 0 ) as cumulative_recovery_reserve,
coalesce ( sum( gross_payment ) over  (  partition by claim_number , peril order by transaction_date  rows  unbounded preceding  ) ,0 ) as cumulative_gross_payment,
coalesce ( sum( recovery ) over  (  partition by claim_number , peril order by transaction_date  rows  unbounded preceding  ) , 0 ) as cumulative_recovery
from transaction_day_level
),
--Creating the capped , excess cumulative amount and gross amounts
transaction_cumulative_cap_exc as (
select  * ,
abs (  cumulative_gross_payment ) +  abs ( cumulative_recovery ) + abs ( cumulative_gross_reserve ) + abs ( cumulative_recovery_reserve ) as cumulative_gross_inc,
case when cumulative_pay  > threshold then threshold else  cumulative_pay end as cumulative_pay_capped,
case when cumulative_pay  > threshold then cumulative_pay -  threshold else  0 end as cumulative_pay_excess,
case when cumulative_est  > threshold then threshold else  cumulative_est end as cumulative_est_capped,
case when cumulative_est  > threshold then cumulative_est -  threshold else  0 end as cumulative_est_excess,
case when cumulative_inc  > threshold then threshold else  cumulative_inc end as cumulative_inc_capped,
case when cumulative_inc  > threshold then cumulative_inc -  threshold else  0 end as cumulative_inc_excess,
case when cumulative_gross_reserve  > threshold then threshold else  cumulative_gross_reserve end as cumulative_gross_reserve_capped,
case when cumulative_gross_reserve  > threshold then cumulative_gross_reserve -  threshold else  0 end as cumulative_gross_reserve_excess,
case when cumulative_recovery_reserve  > threshold then threshold else  cumulative_recovery_reserve end as cumulative_recovery_reserve_capped,
case when cumulative_recovery_reserve  > threshold then cumulative_recovery_reserve -  threshold else  0 end as cumulative_recovery_reserve_excess
from transaction_cumulative ),

-- Round numbers only
transaction_cumulative_round as (
select claim_number,
rcp_code,
policy_source_system,
row_date,
dev_period,
threshold,
brand,
cover_type,
claim_system,
peril,
transaction_date,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month,
round(pay,2) as pay,
round(est,2) as est,
round(inc,2) as inc,
round(min_gross_reserve,2) as min_gross_reserve,
round(min_recovery_reserve,2) as min_recovery_reserve,
txn_seq,
round(cumulative_pay,2) as cumulative_pay,
round(cumulative_est,2) as cumulative_est,
round(cumulative_inc,2) as cumulative_inc,
round(cumulative_gross_reserve,2) as cumulative_gross_reserve,
round(cumulative_recovery_reserve,2) as cumulative_recovery_reserve,
round(cumulative_gross_payment,2) as cumulative_gross_payment,
round(cumulative_recovery,2) as cumulative_recovery,
round(cumulative_gross_inc,2) as cumulative_gross_inc,
round(cumulative_pay_capped,2) as cumulative_pay_capped,
round(cumulative_pay_excess,2) as cumulative_pay_excess,
round(cumulative_est_capped,2) as cumulative_est_capped,
round(cumulative_est_excess,2) as cumulative_est_excess,
round(cumulative_inc_capped,2) as cumulative_inc_capped,
round(cumulative_inc_excess,2) as cumulative_inc_excess,
round(cumulative_gross_reserve_capped,2) as cumulative_gross_reserve_capped,
round(cumulative_gross_reserve_excess,2) as cumulative_gross_reserve_excess,
round(cumulative_recovery_reserve_capped,2) as cumulative_recovery_reserve_capped,
round(cumulative_recovery_reserve_excess,2) as cumulative_recovery_reserve_excess
 
from transaction_cumulative_cap_exc
),
--Derving the financial metric at claim level
financial_metric_flag as
(
select
claim_number,
rcp_code,
policy_source_system,
row_date,
dev_period,
threshold,
brand,
cover_type,
claim_system,
peril,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month,
transaction_date,
txn_seq,
pay,
est,
inc,
cumulative_pay,
cumulative_est,
cumulative_inc,
cumulative_inc_capped,
cumulative_inc_excess,
--FINN
case when txn_seq = 1 then txn_seq else  0 end as  FINN,
--INCN
case when cumulative_gross_inc !=  0  and lag  (cumulative_gross_inc) over  ( partition by  claim_number  , peril  order by   transaction_date )  is null then 1
when  cumulative_gross_inc !=  0  and lag  (cumulative_gross_inc) over  ( partition by  claim_number  , peril  order by   transaction_date ) = 0 then 1
when  cumulative_gross_inc =  0 and  lag  (cumulative_gross_inc) over  ( partition by  claim_number  , peril  order by   transaction_date ) != 0  then -1
else 0 end as INCN,
--SACN
case
  when cumulative_gross_inc != 0 and cumulative_gross_reserve = 0 and cumulative_recovery_reserve = 0 and lag(cumulative_gross_inc,1) over (partition by claim_number , peril  order by   transaction_date ) is null  then 1
  when cumulative_gross_inc != 0 and cumulative_gross_reserve = 0 and cumulative_recovery_reserve = 0 and ((lag(cumulative_gross_reserve,1) over (partition by claim_number , peril  order by   transaction_date ) != 0) or (lag(cumulative_recovery_reserve,1) over (partition by claim_number , peril  order by   transaction_date ) != 0)) then 1
  when ((cumulative_gross_reserve != 0) or (cumulative_recovery_reserve != 0)) and lag(cumulative_gross_inc,1) over (partition by claim_number , peril  order by   transaction_date ) != 0 and lag(cumulative_gross_reserve,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_recovery_reserve,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 then -1
  when cumulative_gross_inc != 0 and  cumulative_gross_reserve = 0 and cumulative_recovery_reserve = 0  and lag(cumulative_gross_inc,1) over (partition by claim_number , peril  order by   transaction_date ) = 0  then 1
  when cumulative_gross_inc = 0 and lag(cumulative_gross_inc,1) over (partition by claim_number , peril  order by   transaction_date ) != 0 and lag(cumulative_gross_reserve,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_recovery_reserve,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 then -1
  else 0 end as SACN,
--SANN
  case when cumulative_gross_inc = 0 and lag(cumulative_gross_inc,1) over (partition by claim_number , peril  order by   transaction_date ) is null and cumulative_gross_reserve = 0 and cumulative_recovery_reserve = 0 and ((min_gross_reserve != 0) or (min_recovery_reserve != 0)) then 1
  when cumulative_gross_inc = 0 and lag(cumulative_gross_inc,1) over (partition by claim_number , peril  order by   transaction_date ) != 0 and cumulative_gross_reserve = 0 and cumulative_recovery_reserve = 0 then 1
  when cumulative_gross_inc != 0 and lag(cumulative_gross_inc,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_gross_reserve,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_recovery_reserve,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 then -1
  else 0 end as SANN,
  case when cumulative_inc <=  threshold then cumulative_inc
  when  cumulative_inc > threshold then threshold
  else 0 end as capped_mov,
  case when cumulative_inc <=  threshold then 0
  when  cumulative_inc > threshold then cumulative_inc - threshold
  else 0 end as excess_mov,
-- CAPPED & EXCESS BI Metrics
--FINN
case when coalesce(cumulative_inc_excess, 0 ) > 0
and  coalesce ( lag(cumulative_inc_excess) over (partition by  claim_number, peril  order by transaction_date) , 0 ) = 0
and   coalesce ( max ( cumulative_inc_excess ) over (partition by  claim_number, peril  order by transaction_date rows BETWEEN unbounded preceding  and 1 preceding ) , 0 )  = 0  then 1 else 0 end as excess_flag,
--INC
coalesce(cumulative_inc_capped,0) - coalesce (lag(cumulative_inc_capped)  over (partition by claim_number, peril  order by transaction_date) , 0 )  as inc_capped,
coalesce(cumulative_inc_excess,0) - coalesce (lag(cumulative_inc_excess)  over (partition by claim_number, peril  order by transaction_date) , 0 )  as inc_excess,
--EST
coalesce(cumulative_est_capped,0) - coalesce (lag(cumulative_est_capped)  over (partition by claim_number, peril  order by transaction_date) , 0 )  as est_capped,
coalesce(cumulative_est_excess,0) - coalesce (lag(cumulative_est_excess)  over (partition by claim_number, peril  order by transaction_date) , 0 )  as est_excess,
--PAY
coalesce(cumulative_pay_capped,0) - coalesce (lag(cumulative_pay_capped)  over (partition by claim_number, peril  order by transaction_date) , 0 )  as pay_capped,
coalesce(cumulative_pay_excess,0) - coalesce (lag(cumulative_pay_excess)  over (partition by claim_number, peril  order by transaction_date) , 0 )  as pay_excess,
--INCN
case when cumulative_inc_capped !=  0  and lag  (cumulative_inc_capped) over  ( partition by  claim_number  , peril  order by   transaction_date )  is null then 1
when  cumulative_inc_capped !=  0  and lag  (cumulative_inc_capped) over  ( partition by  claim_number  , peril  order by   transaction_date ) = 0 then 1
when  cumulative_inc_capped =  0 and  lag  (cumulative_inc_capped) over  ( partition by  claim_number  , peril  order by   transaction_date ) != 0  then -1
else 0 end as INCN_capped,
case when cumulative_inc_excess !=  0  and lag  (cumulative_inc_excess) over  ( partition by  claim_number  , peril  order by   transaction_date )  is null then 1
when  cumulative_inc_excess !=  0  and lag  (cumulative_inc_excess) over  ( partition by  claim_number  , peril  order by   transaction_date ) = 0 then 1
when  cumulative_inc_excess =  0 and  lag  (cumulative_inc_excess) over  ( partition by  claim_number  , peril  order by   transaction_date ) != 0  then -1
else 0 end as INCN_excess,
--SACN
case
  when cumulative_inc_capped != 0 and cumulative_gross_reserve_capped = 0 and cumulative_recovery_reserve_capped = 0 and lag(cumulative_inc_capped,1) over (partition by claim_number , peril  order by   transaction_date ) is null  then 1
  when cumulative_inc_capped != 0 and cumulative_gross_reserve_capped = 0 and cumulative_recovery_reserve_capped = 0 and ((lag(cumulative_gross_reserve_capped,1) over (partition by claim_number , peril  order by   transaction_date ) != 0) or (lag(cumulative_recovery_reserve_capped,1) over (partition by claim_number , peril  order by   transaction_date ) != 0)) then 1
  when ((cumulative_gross_reserve_capped != 0) or (cumulative_recovery_reserve_capped != 0)) and lag(cumulative_inc_capped,1) over (partition by claim_number , peril  order by   transaction_date ) != 0 and lag(cumulative_gross_reserve_capped,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_recovery_reserve_capped,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 then -1
  when cumulative_inc_capped != 0 and  cumulative_gross_reserve_capped = 0 and cumulative_recovery_reserve_capped = 0  and lag(cumulative_inc_capped,1) over (partition by claim_number , peril  order by   transaction_date ) = 0  then 1
  when cumulative_inc_capped = 0 and lag(cumulative_inc_capped,1) over (partition by claim_number , peril  order by   transaction_date ) != 0 and lag(cumulative_gross_reserve_capped,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_recovery_reserve_capped,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 then -1
  else 0 end as SACN_capped,
case
  when cumulative_inc_excess != 0 and cumulative_gross_reserve_excess = 0 and cumulative_recovery_reserve_excess = 0 and lag(cumulative_inc_excess,1) over (partition by claim_number , peril  order by   transaction_date ) is null  then 1
  when cumulative_inc_excess != 0 and cumulative_gross_reserve_excess = 0 and cumulative_recovery_reserve_excess = 0 and ((lag(cumulative_gross_reserve_excess,1) over (partition by claim_number , peril  order by   transaction_date ) != 0) or (lag(cumulative_recovery_reserve_excess,1) over (partition by claim_number , peril  order by   transaction_date ) != 0)) then 1
  when ((cumulative_gross_reserve_excess != 0) or (cumulative_recovery_reserve_excess != 0)) and lag(cumulative_inc_excess,1) over (partition by claim_number , peril  order by   transaction_date ) != 0 and lag(cumulative_gross_reserve_excess,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_recovery_reserve_excess,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 then -1
  when cumulative_inc_excess != 0 and  cumulative_gross_reserve_excess = 0 and cumulative_recovery_reserve_excess = 0  and lag(cumulative_inc_excess,1) over (partition by claim_number , peril  order by   transaction_date ) = 0  then 1
  when cumulative_inc_excess = 0 and lag(cumulative_inc_excess,1) over (partition by claim_number , peril  order by   transaction_date ) != 0 and lag(cumulative_gross_reserve_excess,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_recovery_reserve_excess,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 then -1
  else 0 end as SACN_excess,
--SANN
  case when cumulative_inc_capped = 0 and lag(cumulative_inc_capped,1) over (partition by claim_number , peril  order by   transaction_date ) is null and cumulative_gross_reserve_capped = 0 and cumulative_recovery_reserve_capped = 0 and ((min_gross_reserve != 0) or (min_recovery_reserve != 0)) then 1
  when cumulative_inc_capped = 0 and lag(cumulative_inc_capped,1) over (partition by claim_number , peril  order by   transaction_date ) != 0 and cumulative_gross_reserve_capped = 0 and cumulative_recovery_reserve_capped = 0 then 1
  when cumulative_inc_capped != 0 and lag(cumulative_inc_capped,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_gross_reserve_capped,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_recovery_reserve_capped,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 then -1
  else 0 end as SANN_capped,
  case when cumulative_inc_excess = 0 and lag(cumulative_inc_excess,1) over (partition by claim_number , peril  order by   transaction_date ) is null and cumulative_gross_reserve_excess = 0 and cumulative_recovery_reserve_excess = 0 and ((min_gross_reserve != 0) or (min_recovery_reserve != 0)) and min ( cumulative_inc_excess ) over ( partition by claim_number  , peril  order by   transaction_date rows  unbounded preceding ) > 0 then 1
  when cumulative_inc_excess = 0 and lag(cumulative_inc_excess,1) over (partition by claim_number , peril  order by   transaction_date ) != 0 and cumulative_gross_reserve_excess = 0 and cumulative_recovery_reserve_excess = 0 then 1
  when cumulative_inc_excess != 0 and lag(cumulative_inc_excess,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_gross_reserve_excess,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 and lag(cumulative_recovery_reserve_excess,1) over (partition by claim_number , peril  order by   transaction_date ) = 0 then -1
  else 0 end as SANN_excess
from transaction_cumulative_round
),
--select  * from financial_metric_flag;
financial_metric_claim_level as  (
select
claim_number,
transaction_date,
peril,
txn_seq,
'UKI':: text as underwriter,
brand,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month,
rcp_code,
claim_system,
policy_source_system,
'CM':: text as CLASS,
cover_type,
row_date,
dev_period,
--PAY MOV
case when peril = 'AD' then pay else 0 END as ADGR_PAY_MOV,
case when peril = 'Theft' then pay else 0 END as THFTM_PAY_MOV,
case when peril = 'TP' then pay else 0 END as TPPD_PAY_MOV,
case when peril = 'Windscreen' then pay else 0 END as WSCN_PAY_MOV,
case when peril = 'BI'  then pay_capped else 0 END as BINJ_CAP_PAY_MOV,
case when peril = 'BI'  then pay_excess else 0 END as BINJ_XS_PAY_MOV,
--EST MOV
case when peril = 'AD' then est else 0 END as ADGR_EST_MOV,
case when peril = 'Theft' then est else 0 END as THFTM_EST_MOV,
case when peril = 'TP' then  est else 0 END as TPPD_EST_MOV,
case when peril = 'Windscreen' then est else 0 END as WSCN_EST_MOV,
case when peril = 'BI'  then est_capped else 0 END as BINJ_CAP_EST_MOV,
case when peril = 'BI'  then est_excess else 0 END as BINJ_XS_EST_MOV,
--INC MOV
case when peril = 'AD' then inc else 0 END as ADGR_INC_MOV,
case when peril = 'Theft' then inc else 0 END as THFTM_INC_MOV,
case when peril = 'TP' then inc else 0 END as TPPD_INC_MOV,
case when peril = 'Windscreen' then inc else 0 END as WSCN_INC_MOV,
case when peril = 'BI'  then inc_capped else 0 END as BINJ_CAP_INC_MOV,
case when peril = 'BI'  then inc_excess else 0 END as BINJ_XS_INC_MOV,
--FINN MOV
case when peril = 'AD' then FINN else 0 END as ADGR_FINN_MOV,
case when peril = 'Theft' then FINN else 0 END as THFTM_FINN_MOV,
case when peril = 'TP' then FINN else 0 END as TPPD_FINN_MOV,
case when peril = 'Windscreen' then FINN else 0 END as WSCN_FINN_MOV,
case when peril = 'BI' then FINN else 0 END as BINJ_CAP_FINN_MOV,
case when peril = 'BI' then excess_flag else 0 END as BINJ_XS_FINN_MOV,
--INCN MOV
case when peril = 'AD' then INCN else 0 END as ADGR_INCN_MOV,
case when peril = 'Theft' then INCN else 0 END as THFTM_INCN_MOV,
case when peril = 'TP' then INCN else 0 END as TPPD_INCN_MOV,
case when peril = 'Windscreen' then INCN else 0 END as WSCN_INCN_MOV,
case when peril = 'BI'  then incn_capped else 0 END as BINJ_CAP_INCN_MOV,
case when peril = 'BI'  then incn_excess else 0 END as BINJ_XS_INCN_MOV,
--SANN MOV
case when peril = 'AD' then SANN else 0 END as ADGR_SANN_MOV,
case when peril = 'Theft' then SANN else 0 END as THFTM_SANN_MOV,
case when peril = 'TP' then SANN  else 0 END as TPPD_SANN_MOV,
case when peril = 'Windscreen' then SANN else 0 END as WSCN_SANN_MOV,
case when peril = 'BI'  then sann_capped else 0 END as BINJ_CAP_SANN_MOV,
case when peril = 'BI'  then sann_excess else 0 END as BINJ_XS_SANN_MOV,
--SACN MOV
case when peril = 'AD' then SACN else 0 END as ADGR_SACN_MOV,
case when peril = 'Theft' then SACN else 0 END as THFTM_SACN_MOV,
case when peril = 'TP' then SACN else 0 END as TPPD_SACN_MOV,
case when peril = 'Windscreen' then SACN else 0 END as WSCN_SACN_MOV,
case when peril = 'BI'  then sacn_capped else 0 END as BINJ_CAP_SACN_MOV,
case when peril = 'BI'  then sacn_excess else 0 END as BINJ_XS_SACN_MOV,
--STPA MOV
CASE WHEN peril = 'AD' AND SACN = 1 THEN cumulative_inc
     WHEN peril = 'AD' AND SACN = -1 THEN lag (cumulative_inc) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS ADGR_STPA_MOV,
CASE WHEN peril = 'Theft' AND SACN = 1 THEN cumulative_inc
     WHEN peril = 'Theft' AND SACN = -1 THEN lag (cumulative_inc) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS THFTM_STPA_MOV,
CASE WHEN peril = 'TP' AND SACN = 1 THEN cumulative_inc
     WHEN peril = 'TP' AND SACN = -1 THEN lag (cumulative_inc) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS  TPPD_STPA_MOV,
CASE WHEN peril = 'Windscreen' AND SACN = 1 THEN cumulative_inc
     WHEN peril = 'Windscreen' AND SACN = -1 THEN lag (cumulative_inc) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS WSCN_STPA_MOV,
CASE WHEN peril = 'BI' AND SACN_capped = 1 THEN cumulative_inc_capped
     WHEN peril = 'BI' AND SACN_capped = -1 THEN lag (cumulative_inc_capped) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS BINJ_CAP_STPA_MOV,
CASE WHEN peril = 'BI' AND SACN_excess = 1 THEN cumulative_inc_excess
     WHEN peril = 'BI' AND SACN_excess = -1 THEN lag (cumulative_inc_excess) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS BINJ_XS_STPA_MOV,
--SPCA MOV
CASE WHEN peril = 'AD' AND SACN = 1 THEN capped_mov
     WHEN peril = 'AD' AND SACN = -1 THEN lag (capped_mov) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS ADGR_SPCA_MOV,
CASE WHEN peril = 'Theft' AND SACN = 1 THEN capped_mov
     WHEN peril = 'Theft' AND SACN = -1 THEN lag (capped_mov) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS THFTM_SPCA_MOV,
CASE WHEN peril = 'TP' AND SACN = 1 THEN capped_mov
     WHEN peril = 'TP' AND SACN = -1 THEN lag (capped_mov) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS  TPPD_SPCA_MOV,
CASE WHEN peril = 'Windscreen' AND SACN = 1 THEN capped_mov
     WHEN peril = 'Windscreen' AND SACN = -1 THEN lag (capped_mov) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS WSCN_SPCA_MOV,
CASE WHEN peril = 'BI' AND SACN_capped = 1 THEN cumulative_inc_capped
     WHEN peril = 'BI' AND SACN_capped = -1 THEN lag (cumulative_inc_capped) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS BINJ_CAP_SPCA_MOV,
CASE WHEN peril = 'BI' AND SACN_excess = 1 THEN cumulative_inc_capped
     WHEN peril = 'BI' AND SACN_excess = -1 THEN lag (cumulative_inc_capped) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS BINJ_XS_SPCA_MOV,
--SPXA MOV
CASE WHEN peril = 'AD' AND SACN = 1 THEN excess_mov
     WHEN peril = 'AD' AND SACN = -1 THEN lag (excess_mov) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS ADGR_SPXA_MOV,
CASE WHEN peril = 'Theft' AND SACN = 1 THEN excess_mov
     WHEN peril = 'Theft' AND SACN = -1 THEN lag (excess_mov) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS THFTM_SPXA_MOV,
CASE WHEN peril = 'TP' AND SACN = 1 THEN excess_mov
     WHEN peril = 'TP' AND SACN = -1 THEN lag (excess_mov) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS  TPPD_SPXA_MOV,
CASE WHEN peril = 'Windscreen' AND SACN = 1 THEN excess_mov
     WHEN peril = 'Windscreen' AND SACN = -1 THEN lag (excess_mov) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS WSCN_SPXA_MOV,
CASE WHEN peril = 'BI' AND SACN_capped = 1 THEN cumulative_inc_excess
     WHEN peril = 'BI' AND SACN_capped = -1 THEN lag (cumulative_inc_excess) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS BINJ_CAP_SPXA_MOV,
CASE WHEN peril = 'BI' AND SACN_excess = 1 THEN cumulative_inc_excess
     WHEN peril = 'BI' AND SACN_excess = -1 THEN lag (cumulative_inc_excess) OVER (PARTITION BY claim_number,peril ORDER BY transaction_date)*-1
     ELSE 0
END AS BINJ_XS_SPXA_MOV
from  financial_metric_flag ) ,


--select  * from  financial_metric_claim_level limit 100;

psicle_final_cte as (
select
underwriter,
brand,
rcp_code,
claim_system,
policy_source_system,
class,
cover_type,
row_date,
policy_start_month,
dev_period,
--PAY
SUM(ADGR_PAY_MOV) as ADGR_PAY_MOV,
SUM(THFTM_PAY_MOV) as THFTM_PAY_MOV,
SUM(TPPD_PAY_MOV) as  TPPD_PAY_MOV,
SUM(WSCN_PAY_MOV) as WSCN_PAY_MOV,
SUM(BINJ_CAP_PAY_MOV) as BINJ_CAP_PAY_MOV,
SUM(BINJ_XS_PAY_MOV) as BINJ_XS_PAY_MOV,
--EST
SUM(ADGR_EST_MOV) as ADGR_EST_MOV,
SUM(THFTM_EST_MOV) as THFTM_EST_MOV,
SUM(TPPD_EST_MOV) as  TPPD_EST_MOV,
SUM(WSCN_EST_MOV) as WSCN_EST_MOV,
SUM(BINJ_CAP_EST_MOV) as BINJ_CAP_EST_MOV,
SUM(BINJ_XS_EST_MOV) as BINJ_XS_EST_MOV,
--INC
SUM(ADGR_INC_MOV) as ADGR_INC_MOV,
SUM(THFTM_INC_MOV) as THFTM_INC_MOV,
SUM(TPPD_INC_MOV) as  TPPD_INC_MOV,
SUM(WSCN_INC_MOV) as WSCN_INC_MOV,
SUM(BINJ_CAP_INC_MOV) as BINJ_CAP_INC_MOV,
SUM(BINJ_XS_INC_MOV) as BINJ_XS_INC_MOV,
--FINN
SUM(ADGR_FINN_MOV) as ADGR_FINN_MOV,
SUM(THFTM_FINN_MOV) as THFTM_FINN_MOV,
SUM(TPPD_FINN_MOV) as  TPPD_FINN_MOV,
SUM(WSCN_FINN_MOV) as WSCN_FINN_MOV,
SUM(BINJ_CAP_FINN_MOV) as BINJ_CAP_FINN_MOV,
0 as BINJ_XS_FINN_MOV,
--INCN
SUM(ADGR_INCN_MOV) as ADGR_INCN_MOV,
SUM(THFTM_INCN_MOV) as THFTM_INCN_MOV,
SUM(TPPD_INCN_MOV) as  TPPD_INCN_MOV,
SUM(WSCN_INCN_MOV) as WSCN_INCN_MOV,
SUM(BINJ_CAP_INCN_MOV) as BINJ_CAP_INCN_MOV,
SUM(BINJ_XS_INCN_MOV) as BINJ_XS_INCN_MOV,
--SANN
SUM(ADGR_SANN_MOV) as ADGR_SANN_MOV,
SUM(THFTM_SANN_MOV) as THFTM_SANN_MOV,
SUM(TPPD_SANN_MOV) as  TPPD_SANN_MOV,
SUM(WSCN_SANN_MOV) as WSCN_SANN_MOV,
SUM(BINJ_CAP_SANN_MOV) as BINJ_CAP_SANN_MOV,
0 as BINJ_XS_SANN_MOV,
--SACN
SUM(ADGR_SACN_MOV) as ADGR_SACN_MOV,
SUM(THFTM_SACN_MOV) as THFTM_SACN_MOV,
SUM(TPPD_SACN_MOV) as  TPPD_SACN_MOV,
SUM(WSCN_SACN_MOV) as WSCN_SACN_MOV,
SUM(BINJ_CAP_SACN_MOV) as BINJ_CAP_SACN_MOV,
SUM(BINJ_XS_SACN_MOV) as BINJ_XS_SACN_MOV,
--STPA
SUM(ADGR_STPA_MOV) as ADGR_STPA_MOV,
SUM(THFTM_STPA_MOV) as THFTM_STPA_MOV,
SUM(TPPD_STPA_MOV) as  TPPD_STPA_MOV,
SUM(WSCN_STPA_MOV) as WSCN_STPA_MOV,
SUM(BINJ_CAP_STPA_MOV) as BINJ_CAP_STPA_MOV,
SUM(BINJ_XS_STPA_MOV) as BINJ_XS_STPA_MOV,
--SPCA
SUM(ADGR_SPCA_MOV) as ADGR_SPCA_MOV,
SUM(THFTM_SPCA_MOV) as THFTM_SPCA_MOV,
SUM(TPPD_SPCA_MOV) as  TPPD_SPCA_MOV,
SUM(WSCN_SPCA_MOV) as WSCN_SPCA_MOV,
SUM(BINJ_CAP_SPCA_MOV) as BINJ_CAP_SPCA_MOV,
SUM(BINJ_XS_SPCA_MOV) as BINJ_XS_SPCA_MOV,
--SPXA
SUM(ADGR_SPXA_MOV) as ADGR_SPXA_MOV,
SUM(THFTM_SPXA_MOV) as THFTM_SPXA_MOV,
SUM(TPPD_SPXA_MOV) as  TPPD_SPXA_MOV,
SUM(WSCN_SPXA_MOV) as WSCN_SPXA_MOV,
SUM(BINJ_CAP_SPXA_MOV) as BINJ_CAP_SPXA_MOV,
SUM(BINJ_XS_SPXA_MOV) as BINJ_XS_SPXA_MOV
from financial_metric_claim_level
group by
underwriter,
brand,
rcp_code,
claim_system,
policy_source_system,
class,
cover_type,
row_date,
policy_start_month,
dev_period )
--Final Select statement for the feed
select
cast (  underwriter as varchar(3) )  as uw,
cast (  brand as varchar(20) )  as brand,
cast (  rcp_code  as varchar(20) )  as cob,
cast (  claim_system as varchar(20) )  as  "system" ,  --Need to be renamed to system while unloading
cast (  policy_source_system as varchar(20) )  as policy_system,
cast (  class as varchar(20) )  as  class,
cast (  cover_type as varchar(20) )  cover_type,
cast (  row_date as date )  row_date,
cast (  policy_start_month as date )  policy_start_month,
cast (  dev_period as smallint )  dev_period,
--Pay Metrics
cast (  adgr_pay_mov as decimal(20,2) )   as adgr_pay_mov,
cast (  binj_cap_pay_mov as decimal(20,2) )   as binj_cap_pay_mov,
cast (  binj_xs_pay_mov as decimal(20,2) )   as binj_xs_pay_mov,
cast (  thftm_pay_mov as decimal(20,2) )   as thftm_pay_mov,
cast (  tppd_pay_mov as decimal(20,2) )   as tppd_pay_mov,
cast (  wscn_pay_mov as decimal(20,2) )   as wscn_pay_mov,
--Est Metrics
cast (  adgr_est_mov as decimal(20,2) )   as adgr_est_mov,
cast (  binj_cap_est_mov as decimal(20,2) )   as binj_cap_est_mov,
cast (  binj_xs_est_mov as decimal(20,2) )   as binj_xs_est_mov,
cast (  thftm_est_mov as decimal(20,2) )   as thftm_est_mov,
cast (  tppd_est_mov as decimal(20,2) )   as tppd_est_mov,
cast (  wscn_est_mov as decimal(20,2) )   as wscn_est_mov,
--Inc Metrics
cast (  adgr_inc_mov as decimal(20,2) )   as adgr_inc_mov,
cast (  binj_cap_inc_mov as decimal(20,2) )   as binj_cap_inc_mov,
cast (  binj_xs_inc_mov as decimal(20,2) )   as binj_xs_inc_mov,
cast (  thftm_inc_mov as decimal(20,2) )   as thftm_inc_mov,
cast (  tppd_inc_mov as decimal(20,2) )   as tppd_inc_mov,
cast (  wscn_inc_mov as decimal(20,2) )   as wscn_inc_mov,
--incn Metrics
cast (  adgr_incn_mov as integer )   as adgr_incn_mov,
cast (  binj_cap_incn_mov as integer )   as binj_cap_incn_mov,
cast (  binj_xs_incn_mov as integer )   as binj_xs_incn_mov,
cast (  thftm_incn_mov as integer )   as thftm_incn_mov,
cast (  tppd_incn_mov as integer )   as tppd_incn_mov,
cast (  wscn_incn_mov as integer )   as wscn_incn_mov,
--finn Metrics
cast ( adgr_finn_mov  as integer )  as adgr_finn_mov,
cast ( binj_cap_finn_mov  as integer )  as binj_cap_finn_mov,
cast ( binj_xs_finn_mov  as integer )  as binj_xs_finn_mov,
cast ( thftm_finn_mov  as integer )  as thftm_finn_mov,
cast ( tppd_finn_mov  as integer )  as tppd_finn_mov,
cast ( wscn_finn_mov  as integer ) as wscn_finn_mov,
--sann Metrics
cast ( adgr_sann_mov  as integer ) as adgr_sann_mov,
cast ( binj_cap_sann_mov  as integer ) as binj_cap_sann_mov,
cast ( binj_xs_sann_mov  as integer ) as binj_xs_sann_mov,
cast ( thftm_sann_mov  as integer ) as thftm_sann_mov,
cast ( tppd_sann_mov  as integer ) as tppd_sann_mov,
cast ( wscn_sann_mov  as integer ) as wscn_sann_mov,
--sacn Metrics
cast ( adgr_sacn_mov  as integer ) as adgr_sacn_mov,
cast ( binj_cap_sacn_mov  as integer ) as binj_cap_sacn_mov,
cast ( binj_xs_sacn_mov  as integer ) as binj_xs_sacn_mov,
cast ( thftm_sacn_mov  as integer ) as thftm_sacn_mov,
cast ( tppd_sacn_mov  as integer ) as tppd_sacn_mov,
cast ( wscn_sacn_mov  as integer ) as wscn_sacn_mov,
--stpa Metrics
cast ( adgr_stpa_mov as decimal(20,2) ) as adgr_stpa_mov,
cast ( binj_cap_stpa_mov as decimal(20,2) ) as binj_cap_stpa_mov,
cast ( binj_xs_stpa_mov as decimal(20,2) ) as binj_xs_stpa_mov,
cast ( thftm_stpa_mov as decimal(20,2) ) as thftm_stpa_mov,
cast ( tppd_stpa_mov  as decimal(20,2) ) as tppd_stpa_mov,
cast ( wscn_stpa_mov  as decimal(20,2) ) as wscn_stpa_mov,
--spca Metrics
cast ( adgr_spca_mov  as decimal(20,2) ) as adgr_spca_mov,
cast ( binj_cap_spca_mov  as decimal(20,2) ) as binj_cap_spca_mov,
cast ( binj_xs_spca_mov  as decimal(20,2) ) as binj_xs_spca_mov,
cast ( thftm_spca_mov  as decimal(20,2) ) as thftm_spca_mov,
cast ( tppd_spca_mov  as decimal(20,2) ) as tppd_spca_mov,
cast ( wscn_spca_mov  as decimal(20,2) ) as wscn_spca_mov,
--spxa Metrics
cast ( adgr_spxa_mov  as decimal(20,2) )  as adgr_spxa_mov,
cast ( binj_cap_spxa_mov  as decimal(20,2) )  as binj_cap_spxa_mov,
cast ( binj_xs_spxa_mov  as decimal(20,2) )  as binj_xs_spxa_mov,
cast ( thftm_spxa_mov  as decimal(20,2) )  as thftm_spxa_mov,
cast ( tppd_spxa_mov  as decimal(20,2) )  as tppd_spxa_mov,
cast ( wscn_spxa_mov  as decimal(20,2) )  as wscn_spxa_mov--,
--TO_DATE('{{ var("max_processed_ts") }}', 'YYYY-MM-DD') AS load_date
from psicle_final_cte
