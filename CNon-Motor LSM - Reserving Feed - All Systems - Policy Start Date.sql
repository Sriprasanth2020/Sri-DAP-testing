/*
WbExport -type=text
-file='P:\Reserving\04_Model_Setup\Data Model Input Process Scripts\LSM Scripts\LOADFILE_CLM_LDATE_COMMERCIAL_NONMOTOR_LSM_WEATHER_DETAIL.csv'
-delimiter=','
-decimal='.'
-quoteChar='"'
-dateFormat='yyyy-MM-dd';
*/

--/*
WbExport -type=text
-file='P:\Reserving\04_Model_Setup\Data Model Input Process Scripts\LSM Scripts\LOADFILE_CLM_LDATE_COMMERCIAL_NONMOTOR_LSM.csv'
-delimiter=','
-decimal='.'
-quoteChar='"'
-dateFormat='yyyy-MM-dd';
--*/

WITH  month_end_claim as
(select distinct *   from (  
select claim_number,
trunc(claim_loss_time) row_date,
class_of_business,
loss_cause_description,
sub_loss_cause_description,
line_of_business,
brand_name,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
rcp_code,
policy_source_system,
scd_start_date,
scd_end_date,
dap_source_system_name,
policy_number,
claim_state,
trunc(reported_date) AS notification_date
FROM dap_lsm_claims_commercial_property.dim_claim_hist 
where retired_flag = 0
and nvl(claim_state,'Draft') != 'Draft'
--and  dap_source_system_name = 'CC10'
and scd_start_date  < date_trunc('month', getdate()) and   scd_end_date >= date_trunc('month', getdate())   -- To bring the records which was active at the last date of the pervious month 

and claim_number in
(
'594328684','594328726','594328734','594328767','594328890','594328916','594329039','594329062','594329187',
'594329195','594329203','594329229','594329641','594329682','594329708','594329864','594330136','594328809',  
'594329096', '594330573'  
)

)  ),

month_end_exposure as
(
select claim_number,
exposure_order,
coverage_sub_type,
exposure_type,
loss_party_type,
exposure_state,
scd_start_date,
scd_end_date,
dap_source_system_name
from dap_lsm_claims_commercial_property.dim_claim_exposure_hist 
where retired_flag = 0
and nvl(exposure_state,'Draft') != 'Draft'
--and  dap_source_system_name = 'CC10'
and scd_start_date  < date_trunc('month', getdate()) and   scd_end_date >= date_trunc('month', getdate()) 
and claim_number in
(
'594328684','594328726','594328734','594328767','594328890','594328916','594329039','594329062','594329187',
'594329195','594329203','594329229','594329641','594329682','594329708','594329864','594330136','594328809',  
'594329096', '594330573'  
)

),

event_list as ( 
SELECT evn_peril,
       evn_event_name,
       evn_event_start_date,
       evn_event_end_date,
       evn_dlg_event_flag,
       evn_changedate,
       dap_processed_timestamp

FROM ext_curation_claims_supplementary_feeds.sup_event_list
where dap_processed_timestamp = ( select max(dap_processed_timestamp) from ext_curation_claims_supplementary_feeds.sup_event_list ) and
evn_dlg_event_flag = 'Y' ), 


-- selecting all the columns from different table for submited transactions at transaction_line Level
transaction_line_level as (
SELECT fctj.claim_number,
       dch.class_of_business as cob,
       dch.policy_source_system,
       dch.policy_effective_date,
       dch.policy_inception_date,
       dch.policy_expiration_date,
       trunc(date_trunc ('month',dch.policy_effective_date)) AS policy_start_month, 
       loss_cal.cal_date AS loss_date,
       trunc(date_trunc ('month',loss_cal.cal_date)) AS row_date,
       datediff (months , trunc(date_trunc ('month',loss_cal.cal_date))  ,  trunc(date_trunc ('month', trans_cal.cal_date )) ) + 1 as dev_period,
       round (case when  loss_cal.cal_year_no = 1999  THEN 50000  ELSE   (1.07 ^  (loss_cal.cal_year_no - 1999)) * 50000   end ,4) as  threshold,
       fctj.claim_transaction_status_id,
       db.brand_code  AS brand,
       fctj.dap_source_system_name AS claim_system,
       trans_cal.cal_date AS transaction_date,
       dct.payment_type,
       dceh.coverage_sub_type,
       dch.policy_number,
       dch.claim_state,
       loss_cause_description,
       dch.notification_date,
       dceh.exposure_state,
       dceh.exposure_order,
       dceh.exposure_type,
       dceh.loss_party_type,
       CASE WHEN dct.payment_type = 'Payment' THEN  jda_base_currency_amount ELSE  0  END as payment_with_recovery,
       CASE WHEN dct.payment_type = 'Reserve' THEN  jda_base_currency_amount ELSE  0  END as reserve_with_recovery,
       CASE WHEN dct.payment_type = 'Recovery'  AND  UPPER(jda_line_category_code) in ( 'SALVAGE_INS' , 'SUBROG_INS' , 'EXCESSCOLL_INS' , 'OTHERCOLL_INS' )  THEN  jda_base_currency_amount
            WHEN dct.payment_type = 'Recovery'  THEN jda_base_currency_amount * -1  
             ELSE  0 
       END as recovery,
       CASE WHEN dct.payment_type = 'RecoveryReserve' THEN   jda_base_currency_amount  * -1  ELSE  0  END as recovery_reserve,
       CASE WHEN loss_cause_description   = 'Liability' THEN 'GL' 
       WHEN  loss_cause_description =  'Personal Accident/Assault' THEN 'PA'
       WHEN upper(dceh.coverage_sub_type) like '%FINANCIAL LOSS%'   THEN 'PecL'
       ELSE  'CP' END as class,
      
       CASE  WHEN loss_cause_description   = 'Liability'   then 'GLIA'
       when loss_cause_description =  'Personal Accident/Assault' then 'PACC' 
       when loss_cause_description in ( 'Fire' , 'Lightning' , 'Explosion' )  then 'FIRE'
       when loss_cause_description in ( 'Fire' , 'Lightning' , 'Explosion' )  then 'FIRE' 
       when loss_cause_description in  ( 'Escape of Water'  , 'Escape of Oil' , 'Accidental Discharge of Sprinklers' , 'Leakage Beverage',
       'Leakage Fuel' , 'Loss of Oil or Metered Water' ) THEN 'ESCW'
       when loss_cause_description in ( 'Flood' , 'Storm' ) THEN 'WTHR'
       ELSE 'SMLP'
       END as peril_grouping,

       CASE WHEN sub_loss_cause_description in (  'Defective Workmanship - Disease' , 'Disease' , 'Employers Liability - Industrial Disease' ,
       'Public Liability - Disease', 'Industrial Disease' ) THEN  1 ELSE 0 END as latents_flag,
           
        CASE WHEN loss_cause_description in  ( 'Escape of Water'  , 'Escape of Oil' , 'Accidental Discharge of Sprinklers' , 'Leakage Beverage',
       'Leakage Fuel' , 'Loss of Oil or Metered Water' ) THEN 'EOW'
       WHEN  loss_cause_description ='Flood'  THEN 'FLOOD'
       WHEN loss_cause_description ='Storm'  THEN 'STORM' 
       end as event_join_peril,
       
       CASE WHEN dch.class_of_business in ('700', '730') then 'DFB03'
       WHEN dch.class_of_business in ('701', '731', '740') then 'DFB04'
       WHEN dch.class_of_business in ('190', '400', '414', '535') then 'C4B02'
       else 'XXXXX' end as sub_brand
               
FROM dap_lsm_claims_commercial_property.fact_claims_transaction_journey fctj
  INNER JOIN month_end_claim as dch ON fctj.claim_number = dch.claim_number
  LEFT JOIN dap_lsm_claims_commercial_property.dim_claim_exposure_hist as exp_hist ON fctj.claim_exposure_hist_id = exp_hist.claim_exposure_hist_id
  LEFT JOIN month_end_exposure as dceh ON fctj.claim_number = dceh.claim_number and exp_hist.exposure_order = dceh.exposure_order
  LEFT JOIN dap_lsm_claims_commercial.dim_brand db ON db.brand_id = fctj.brand_id
  LEFT JOIN dap_lsm_claims_commercial_property.dim_cost_type dct ON dct.cost_type_id = fctj.cost_type_id
  LEFT JOIN dap_lsm_claims_commercial.dim_calendar loss_cal ON loss_cal.date_id = fctj.loss_date_id
  LEFT JOIN dap_lsm_claims_commercial.dim_calendar trans_cal ON trans_cal.date_id = fctj.transaction_date_id
WHERE claim_transaction_status_id = 1 and jda_transaction_status_cd = 'submitted' and coalesce(jda_line_of_business,'blah') not in ('Legal Protection')  and  transaction_date <= trunc(date_trunc('month',getdate()) -1)-- and fctj.dap_source_system_name = 'CC5'
and fctj.claim_number in
(
'594328684','594328726','594328734','594328767','594328890','594328916','594329039','594329062','594329187',
'594329195','594329203','594329229','594329641','594329682','594329708','594329864','594330136','594328809',  
'594329096', '594330573'  
)

),

--adding event flag & Peril logic 
transaction_line_level_peril  as 
( 
SELECT   * ,  case when evn_event_name is not null then 1 else 0 end as large_event_flag ,
        CASE WHEN latents_flag = 1 THEN 'Latents'
        WHEN  peril_grouping = 'GLIA'  AND   coverage_sub_type = 'Employers Liability Commercial - Bodily Injury' THEN 'ELOT'
        WHEN  peril_grouping = 'GLIA'  AND   coverage_sub_type = 'Public Liability - Bodily Injury' THEN 'PLIN'
        WHEN  peril_grouping = 'GLIA'  AND   coverage_sub_type = 'Products Liability - Bodily Injury' THEN 'PLIN'
        WHEN  peril_grouping = 'GLIA'   THEN 'PLPR'
        WHEN  peril_grouping = 'ESCW'  and  large_event_flag  = 1   THEN 'EWEVH'
        WHEN  peril_grouping = 'ESCW'  and  large_event_flag  = 0    THEN 'EWATH'
        WHEN  peril_grouping = 'WTHR' and  large_event_flag  = 1   THEN 'FLEVH'
        WHEN  peril_grouping = 'WTHR' and  large_event_flag  = 0   THEN 'FLATH'
        ELSE peril_grouping
        end as peril

 FROM  transaction_line_level  
LEFT JOIN event_list  on event_list.evn_peril = event_join_peril  and  loss_date  >=  evn_event_start_date and   loss_date  <= evn_event_end_date  
),

-- Do not delete as needed for AVIVA weather output
/*
select brand,
  class,
  row_date,
  dev_period,
  claim_number,
  policy_number,
  cob,
  sub_brand,
  loss_cause_description,
  peril_grouping,
  notification_date,
  claim_state,
  exposure_state,
  exposure_type,
  loss_party_type,
  exposure_order,
  loss_date,
  sum(payment_with_recovery) as tot_paid,
  sum(reserve_with_recovery) as tot_reserve,
  sum(recovery) as tot_re_paid,
  sum(recovery_reserve) as tot_re_reserve
  from transaction_line_level_peril
  where peril_grouping in ('WTHR')
  group by brand, class, row_date, dev_period, claim_number,
  policy_number, cob, sub_brand, loss_cause_description, peril_grouping,
  notification_date, claim_state, exposure_state, exposure_type,
  loss_party_type, exposure_order, loss_date
  --limit 100
;
*/

--group transaction to Day Level
transaction_day_level  AS  
( 
select  
claim_number,
cob,
sub_brand,
policy_source_system,
row_date,
dev_period,
threshold,
brand,
claim_system,
transaction_date,
peril,
class,
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
SUM ( recovery_reserve ) as recovery_reserve
FROM  transaction_line_level_peril 
GROUP BY 
claim_number,
cob,
sub_brand,
policy_source_system,
row_date,
dev_period,
threshold,
brand,
claim_system,
transaction_date,
peril,
class,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month ),

--Cumulative values at claim , Peril level 
transaction_cumulative as 
( 
select  
claim_number, 
cob,
sub_brand,
policy_source_system,
row_date,
dev_period,
threshold,
brand,
claim_system,
peril, 
class,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month,
transaction_date,
pay,
est,
inc,
coalesce ( sum( pay ) over ( partition by claim_number , peril , class order by transaction_date rows  unbounded preceding  ), 0)  as  cumulative_pay ,
coalesce ( sum( inc ) over ( partition by claim_number , peril , class  order by transaction_date  rows unbounded preceding   ) , 0 ) as cumulative_inc,
coalesce ( sum( gross_reserve ) over  (  partition by claim_number , peril , class order by transaction_date  rows  unbounded preceding  ) ,0 ) as cumulative_gross_reserve,
coalesce ( sum( recovery_reserve ) over  (  partition by claim_number , peril , class order by transaction_date  rows  unbounded preceding  ) , 0 ) as cumulative_recovery_reserve,
coalesce ( sum( gross_payment ) over  (  partition by claim_number , peril , class order by transaction_date  rows  unbounded preceding  ) ,0 ) as cumulative_gross_payment,
coalesce ( sum( recovery ) over  (  partition by claim_number , peril , class order by transaction_date  rows  unbounded preceding  ) , 0 ) as cumulative_recovery
from transaction_day_level 
),
--Creating the capped and excess cumulative amount 
transaction_cumulative_cap_exc as ( 
select  * , 
abs (  cumulative_gross_payment ) +  abs ( cumulative_recovery ) + abs ( cumulative_gross_reserve ) + abs ( cumulative_recovery_reserve ) as cumulative_gross_inc,
case when cumulative_gross_inc >  threshold   then threshold else   cumulative_gross_inc end as   cumulative_gross_inc_capped,
case when cumulative_pay  > threshold then threshold else  cumulative_pay end as cumulative_pay_capped,
case when cumulative_pay  > threshold then cumulative_pay -  threshold else  0 end as cumulative_pay_excess,
case when cumulative_inc  > threshold then threshold else  cumulative_inc end as cumulative_inc_capped,
case when cumulative_inc  > threshold then cumulative_inc -  threshold else  0 end as cumulative_inc_excess
from transaction_cumulative ), 
--Derving the financial metric at claim level 
financial_metric_flag as 
(
select  
claim_number, 
cob,
sub_brand,
policy_source_system,
row_date,
dev_period,
threshold,
brand,
claim_system,
peril, 
class,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month,
transaction_date,
pay,
est,
inc,
cumulative_pay,
cumulative_inc,
cumulative_inc_capped,
cumulative_inc_excess,
--INCN
case when cumulative_gross_inc !=  0  and lag  (cumulative_gross_inc) over  ( partition by  claim_number  , peril , class  order by   transaction_date )  is null then 1 
when  cumulative_gross_inc !=  0  and lag  (cumulative_gross_inc) over  ( partition by  claim_number  , peril , class order by   transaction_date ) = 0 then 1 
when  cumulative_gross_inc =  0 and  lag  (cumulative_gross_inc) over  ( partition by  claim_number  , peril , class  order by   transaction_date ) != 0  then -1 
else 0 end as INCN,
-- CAPPED & EXCESS Metrics
--INC 
coalesce(cumulative_inc_capped,0) - coalesce (lag(cumulative_inc_capped)  over (partition by claim_number, peril , class order by transaction_date) , 0 )  as inc_capped,
coalesce(cumulative_inc_excess,0) - coalesce (lag(cumulative_inc_excess)  over (partition by claim_number, peril , class order by transaction_date) , 0 )  as inc_excess,
--PAY
coalesce(cumulative_pay_capped,0) - coalesce (lag(cumulative_pay_capped)  over (partition by claim_number, peril , class order by transaction_date) , 0 )  as pay_capped,
coalesce(cumulative_pay_excess,0) - coalesce (lag(cumulative_pay_excess)  over (partition by claim_number, peril , class  order by transaction_date) , 0 )  as pay_excess,
--INCN
case when cumulative_gross_inc_capped !=  0  and lag  (cumulative_gross_inc_capped) over  ( partition by  claim_number  , peril , class order by   transaction_date )  is null then 1 
when  cumulative_gross_inc_capped !=  0  and lag  (cumulative_gross_inc_capped) over  ( partition by  claim_number  , peril , class  order by   transaction_date ) = 0 then 1 
when  cumulative_gross_inc_capped =  0 and  lag  (cumulative_gross_inc_capped) over  ( partition by  claim_number  , peril , class  order by   transaction_date ) != 0  then -1 
else 0 end as INCN_capped,
case when cumulative_inc_excess !=  0  and lag  (cumulative_inc_excess) over  ( partition by  claim_number  , peril , class order by   transaction_date )  is null then 1 
when  cumulative_inc_excess !=  0  and lag  (cumulative_inc_excess) over  ( partition by  claim_number  , peril , class order by   transaction_date ) = 0 then 1 
when  cumulative_inc_excess =  0 and  lag  (cumulative_inc_excess) over  ( partition by  claim_number  , peril , class order by   transaction_date ) != 0  then -1 
else 0 end as INCN_excess
from transaction_cumulative_cap_exc 
),

financial_metric_claim_level as  ( 
select  
claim_number,
transaction_date,
peril,
'UKI':: text as underwriter,
brand,
cob,
sub_brand,
claim_system,
policy_source_system,
class ,
policy_effective_date,
policy_inception_date,
policy_expiration_date,
policy_start_month,
row_date,
dev_period,
--PAY MOV
case when peril = 'ACDMtot' then pay else 0 END as ACDMtot_PAY,    ---ACDMtot is considered as SMLP and values are populated in OTHRtot
case when peril = 'EWATH' then pay else 0 END as EWATH_PAY,
case when peril = 'EWEVH' then pay else 0 END as EWEVH_PAY,
case when peril = 'FLATH' then pay else 0 END as FLATH_PAY,
case when peril = 'FLEVH' then pay else 0 END as FLEVH_PAY,
case when peril = 'Latents' then pay else 0 END as Latents_PAY,   
case when peril = 'LIABtot' then pay else 0 END as LIABtot_PAY,     --LIABtot is considered as SMLP and values are populated in OTHRtot
case when peril = 'SMLP' then pay else 0 END as OTHRtot_PAY,
case when peril = 'PACC' then pay else 0 END as PA_PAY,
case when peril = 'STATH' then pay else 0 END as STATH_PAY,
case when peril = 'STEVH' then pay else 0 END as STEVH_PAY,
case when peril = 'SUBStot' then pay else 0 END as SUBStot_PAY,     --SUBStot is considered as SMLP and values are populated in OTHRtot
case when peril = 'THFTtot' then pay else 0 END as THFTtot_PAY,     --THFTtot is considered as SMLP and values are populated in OTHRtot
--EST MOV
case when peril = 'ACDMtot' then est else 0 END as ACDMtot_EST,     --ACDMtot is considered as SMLP and values are populated in OTHRtot
case when peril = 'EWATH' then est else 0 END as EWATH_EST,
case when peril = 'EWEVH' then est else 0 END as EWEVH_EST,
case when peril = 'FLATH' then est else 0 END as FLATH_EST,
case when peril = 'FLEVH' then est else 0 END as FLEVH_EST,
case when peril = 'Latents' then est else 0 END as Latents_EST,
case when peril = 'LIABtot' then est else 0 END as LIABtot_EST,     --LIABtot is considered as SMLP and values are populated in OTHRtot
case when peril = 'SMLP' then est else 0 END as OTHRtot_EST,
case when peril = 'PACC' then est else 0 END as PA_EST,
case when peril = 'STATH' then est else 0 END as STATH_EST,
case when peril = 'STEVH' then est else 0 END as STEVH_EST,
case when peril = 'SUBStot' then est else 0 END as SUBStot_EST,     --SUBStot is considered as SMLP and values are populated in OTHRtot
case when peril = 'THFTtot' then est else 0 END as THFTtot_EST,     --THFTtot is considered as SMLP and values are populated in OTHRtot
--INC MOV
case when peril = 'ACDMtot' then inc else 0 END as ACDMtot_INC,     --ACDMtot is considered as SMLP and values are populated in OTHRtot
case when peril = 'EWATH' then inc else 0 END as EWATH_INC,
case when peril = 'EWEVH' then inc else 0 END as EWEVH_INC,
case when peril = 'FLATH' then inc else 0 END as FLATH_INC,
case when peril = 'FLEVH' then inc else 0 END as FLEVH_INC,
case when peril = 'Latents' then inc else 0 END as Latents_INC,
case when peril = 'LIABtot' then inc else 0 END as LIABtot_INC,     --LIABtot is considered as SMLP and values are populated in OTHRtot
case when peril = 'SMLP' then inc else 0 END as OTHRtot_INC,
case when peril = 'PACC' then inc else 0 END as PA_INC,
case when peril = 'STATH' then inc else 0 END as STATH_INC,
case when peril = 'STEVH' then inc else 0 END as STEVH_INC,
case when peril = 'SUBStot' then inc else 0 END as SUBStot_INC,     --SUBStot is considered as SMLP and values are populated in OTHRtot
case when peril = 'THFTtot' then inc else 0 END as THFTtot_INC,     --THFTtot is considered as SMLP and values are populated in OTHRtot
--INCN MOV
case when peril = 'ACDMtot' then incn else 0 END as ACDMtot_INCN,     --ACDMtot is considered as SMLP and values are populated in OTHRtot
case when peril = 'EWATH' then incn else 0 END as EWATH_INCN,
case when peril = 'EWEVH' then incn else 0 END as EWEVH_INCN,
case when peril = 'FLATH' then incn else 0 END as FLATH_INCN,
case when peril = 'FLEVH' then incn else 0 END as FLEVH_INCN,
case when peril = 'Latents' then incn else 0 END as Latents_INCN,
case when peril = 'LIABtot' then incn else 0 END as LIABtot_INCN,     --LIABtot is considered as SMLP and values are populated in OTHRtot
case when peril = 'SMLP' then incn else 0 END as OTHRtot_INCN,
case when peril = 'PACC' then incn else 0 END as PA_INCN,
case when peril = 'STATH' then incn else 0 END as STATH_INCN,
case when peril = 'STEVH' then incn else 0 END as STEVH_INCN,
case when peril = 'SUBStot' then incn else 0 END as SUBStot_INCN,     --SUBStot is considered as SMLP and values are populated in OTHRtot
case when peril = 'THFTtot' then incn else 0 END as THFTtot_INCN,     --THFTtot is considered as SMLP and values are populated in OTHRtot
--CAP PAY
case when peril = 'ELOT' then pay_capped else 0 END as ELOTCap_CAPPAY,
case when peril = 'FIRE' then pay_capped else 0 END as FICPtot_CAPPAY,
case when peril = 'PLIN' then pay_capped else 0 END as PLINCap_CAPPAY,
case when peril = 'PLPR' then pay_capped else 0 END as PLPRCap_CAPPAY,
--CAP INC
case when peril = 'ELOT' then inc_capped else 0 END as ELOTCap_CAPINC,
case when peril = 'FIRE' then inc_capped else 0 END as FICPtot_CAPINC,
case when peril = 'PLIN' then inc_capped else 0 END as PLINCap_CAPINC,
case when peril = 'PLPR' then inc_capped else 0 END as PLPRCap_CAPINC,
--CAP INCN
case when peril = 'ELOT' then incn_capped else 0 END as ELOTCap_CAPINCN,
case when peril = 'FIRE' then incn_capped else 0 END as FICPtot_CAPINCN,
case when peril = 'PLIN' then incn_capped else 0 END as PLINCap_CAPINCN,
case when peril = 'PLPR' then incn_capped else 0 END as PLPRCap_CAPINCN,
--XS PAY
case when peril = 'ELOT' then pay_excess else 0 END as ELOTEx_XSPAY,
case when peril = 'FIRE' then pay_excess else 0 END as FIEXtot_XSPAY,
case when peril = 'PLIN' then pay_excess else 0 END as PLINEx_XSPAY,
case when peril = 'PLPR' then pay_excess else 0 END as PLPREx_XSPAY,
--XS INC
case when peril = 'ELOT' then inc_excess else 0 END as ELOTEx_XSINC,
case when peril = 'FIRE' then inc_excess else 0 END as FIEXtot_XSINC,
case when peril = 'PLIN' then inc_excess else 0 END as PLINEx_XSINC,
case when peril = 'PLPR' then inc_excess else 0 END as PLPREx_XSINC,
--XS INCN
case when peril = 'ELOT' then incn_excess else 0 END as ELOTEx_XSINCN,
case when peril = 'FIRE' then incn_excess else 0 END as FIEXtot_XSINCN,
case when peril = 'PLIN' then incn_excess else 0 END as PLINEx_XSINCN,
case when peril = 'PLPR' then incn_excess else 0 END as PLPREx_XSINCN
from  financial_metric_flag ) ,


--select *
--from financial_metric_claim_level limit 100;

psicle_final_cte as ( 
select  
underwriter,
brand,
cob,
sub_brand,
claim_system,
policy_source_system,
policy_start_month,
class,
row_date,
dev_period, 
--PAY 
SUM(ACDMtot_PAY) as ACDMtot_PAY,
SUM(EWATH_PAY) as EWATH_PAY,
SUM(EWEVH_PAY) as EWEVH_PAY,
SUM(FLATH_PAY) as FLATH_PAY,
SUM(FLEVH_PAY) as FLEVH_PAY,
SUM(Latents_PAY) as Latents_PAY,
SUM(LIABtot_PAY) as LIABtot_PAY,
SUM(OTHRtot_PAY) as OTHRtot_PAY,
SUM(PA_PAY) as PA_PAY,
SUM(STATH_PAY) as STATH_PAY,
SUM(STEVH_PAY) as STEVH_PAY,
SUM(SUBStot_PAY) as SUBStot_PAY,
SUM(THFTtot_PAY) as THFTtot_PAY,
--EST
SUM(ACDMtot_EST) as ACDMtot_EST,
SUM(EWATH_EST) as EWATH_EST,
SUM(EWEVH_EST) as EWEVH_EST,
SUM(FLATH_EST) as FLATH_EST,
SUM(FLEVH_EST) as FLEVH_EST,
SUM(Latents_EST) as Latents_EST,
SUM(LIABtot_EST) as LIABtot_EST,
SUM(OTHRtot_EST) as OTHRtot_EST,
SUM(PA_EST) as PA_EST,
SUM(STATH_EST) as STATH_EST,
SUM(STEVH_EST) as STEVH_EST,
SUM(SUBStot_EST) as SUBStot_EST,
SUM(THFTtot_EST) as THFTtot_EST,
--INC
SUM(ACDMtot_INC) as ACDMtot_INC,
SUM(EWATH_INC) as EWATH_INC,
SUM(EWEVH_INC) as EWEVH_INC,
SUM(FLATH_INC) as FLATH_INC,
SUM(FLEVH_INC) as FLEVH_INC,
SUM(Latents_INC) as Latents_INC,
SUM(LIABtot_INC) as LIABtot_INC,
SUM(OTHRtot_INC) as OTHRtot_INC,
SUM(PA_INC) as PA_INC,
SUM(STATH_INC) as STATH_INC,
SUM(STEVH_INC) as STEVH_INC,
SUM(SUBStot_INC) as SUBStot_INC,
SUM(THFTtot_INC) as THFTtot_INC,
--INCN
SUM(ACDMtot_INCN) as ACDMtot_INCN,
SUM(EWATH_INCN) as EWATH_INCN,
SUM(EWEVH_INCN) as EWEVH_INCN,
SUM(FLATH_INCN) as FLATH_INCN,
SUM(FLEVH_INCN) as FLEVH_INCN,
SUM(Latents_INCN) as Latents_INCN,
SUM(LIABtot_INCN) as LIABtot_INCN,
SUM(OTHRtot_INCN) as OTHRtot_INCN,
SUM(PA_INCN) as PA_INCN,
SUM(STATH_INCN) as STATH_INCN,
SUM(STEVH_INCN) as STEVH_INCN,
SUM(SUBStot_INCN) as SUBStot_INCN,
SUM(THFTtot_INCN) as THFTtot_INCN,
--CAPPED PAY
SUM(ELOTCap_CAPPAY) as ELOTCap_CAPPAY,
SUM(FICPtot_CAPPAY) as FICPtot_CAPPAY,
SUM(PLINCap_CAPPAY) as PLINCap_CAPPAY,
SUM(PLPRCap_CAPPAY) as PLPRCap_CAPPAY,
--CAPPED INC
SUM(ELOTCap_CAPINC) as ELOTCap_CAPINC,
SUM(FICPtot_CAPINC) as FICPtot_CAPINC,
SUM(PLINCap_CAPINC) as PLINCap_CAPINC,
SUM(PLPRCap_CAPINC) as PLPRCap_CAPINC,
--CAPPED INCN
SUM(ELOTCap_CAPINCN) as ELOTCap_CAPINCN,
SUM(FICPtot_CAPINCN) as FICPtot_CAPINCN,
SUM(PLINCap_CAPINCN) as PLINCap_CAPINCN,
SUM(PLPRCap_CAPINCN)  as PLPRCap_CAPINCN,
--XS PAY
SUM(ELOTEx_XSPAY) as ELOTEx_XSPAY,
SUM(FIEXtot_XSPAY) as FIEXtot_XSPAY,
SUM(PLINEx_XSPAY) as PLINEx_XSPAY,
SUM(PLPREx_XSPAY) as PLPREx_XSPAY,
--XS INC
SUM(ELOTEx_XSINC) as ELOTEx_XSINC,
SUM(FIEXtot_XSINC) as FIEXtot_XSINC,
SUM(PLINEx_XSINC) as PLINEx_XSINC,
SUM(PLPREx_XSINC) as PLPREx_XSINC,
--XS INCN
SUM(ELOTEx_XSINCN) as ELOTEx_XSINCN,
SUM(FIEXtot_XSINCN) as FIEXtot_XSINCN,
SUM(PLINEx_XSINCN) as PLINEx_XSINCN,
SUM(PLPREx_XSINCN) as PLPREx_XSINCN
from financial_metric_claim_level
group by 
underwriter,
brand,
cob,
sub_brand,
claim_system,
policy_source_system,
class,
row_date,
policy_start_month,
dev_period ) 
--Final Select statement for the feed
select 
cast (  underwriter as varchar(3) )  as uw,
cast (  brand as varchar(20) )  as brand, 
cast (  cob  as varchar(20) )  as cob, 
cast (  sub_brand  as varchar(20) )  as sub_brand, 
cast (  claim_system as varchar(20) )  as  "system" ,  --Need to be renamed to system while unloading
cast (  policy_source_system as varchar(20) )  as policy_system,  
cast (  class as varchar(20) )  as  class,  
cast (  row_date as date )  rowdate,
cast (  policy_start_month as date )  policy_start_month,
cast (  dev_period as smallint )  devperiod,
--PAY 
CAST(ACDMtot_PAY as decimal(20,2) ) as ACDMtot_PAY,
CAST(EWATH_PAY as decimal(20,2) ) as EWATH_PAY,
CAST(EWEVH_PAY as decimal(20,2) ) as EWEVH_PAY,
CAST(FLATH_PAY as decimal(20,2) ) as FLATH_PAY,
CAST(FLEVH_PAY as decimal(20,2) ) as FLEVH_PAY,
CAST(Latents_PAY as decimal(20,2) ) as Latents_PAY,
CAST(LIABtot_PAY as decimal(20,2) ) as LIABtot_PAY,
CAST(OTHRtot_PAY as decimal(20,2) ) as OTHRtot_PAY,
CAST(PA_PAY as decimal(20,2) ) as PA_PAY,
CAST(STATH_PAY as decimal(20,2) ) as STATH_PAY,
CAST(STEVH_PAY as decimal(20,2) ) as STEVH_PAY,
CAST(SUBStot_PAY as decimal(20,2) ) as SUBStot_PAY,
CAST(THFTtot_PAY as decimal(20,2) ) as THFTtot_PAY,
--EST
CAST(ACDMtot_EST as decimal(20,2) ) as ACDMtot_EST,
CAST(EWATH_EST as decimal(20,2) ) as EWATH_EST,
CAST(EWEVH_EST as decimal(20,2) ) as EWEVH_EST,
CAST(FLATH_EST as decimal(20,2) ) as FLATH_EST,
CAST(FLEVH_EST as decimal(20,2) ) as FLEVH_EST,
CAST(Latents_EST as decimal(20,2) ) as Latents_EST,
CAST(LIABtot_EST as decimal(20,2) ) as LIABtot_EST,
CAST(OTHRtot_EST as decimal(20,2) ) as OTHRtot_EST,
CAST(PA_EST as decimal(20,2) ) as PA_EST,
CAST(STATH_EST as decimal(20,2) ) as STATH_EST,
CAST(STEVH_EST as decimal(20,2) ) as STEVH_EST,
CAST(SUBStot_EST as decimal(20,2) ) as SUBStot_EST,
CAST(THFTtot_EST as decimal(20,2) ) as THFTtot_EST,
--INC
CAST(ACDMtot_INC as decimal(20,2) ) as ACDMtot_INC,
CAST(EWATH_INC as decimal(20,2) ) as EWATH_INC,
CAST(EWEVH_INC as decimal(20,2) ) as EWEVH_INC,
CAST(FLATH_INC as decimal(20,2) ) as FLATH_INC,
CAST(FLEVH_INC as decimal(20,2) ) as FLEVH_INC,
CAST(Latents_INC as decimal(20,2) ) as Latents_INC,
CAST(LIABtot_INC as decimal(20,2) ) as LIABtot_INC,
CAST(OTHRtot_INC as decimal(20,2) ) as OTHRtot_INC,
CAST(PA_INC as decimal(20,2) ) as PA_INC,
CAST(STATH_INC as decimal(20,2) ) as STATH_INC,
CAST(STEVH_INC as decimal(20,2) ) as STEVH_INC,
CAST(SUBStot_INC as decimal(20,2) ) as SUBStot_INC,
CAST(THFTtot_INC as decimal(20,2) ) as THFTtot_INC,
--INCN
CAST(ACDMtot_INCN as integer ) as ACDMtot_INCN,
CAST(EWATH_INCN as integer ) as EWATH_INCN,
CAST(EWEVH_INCN as integer ) as EWEVH_INCN,
CAST(FLATH_INCN as integer ) as FLATH_INCN,
CAST(FLEVH_INCN as integer ) as FLEVH_INCN,
CAST(Latents_INCN as integer ) as Latents_INCN,
CAST(LIABtot_INCN as integer ) as LIABtot_INCN,
CAST(OTHRtot_INCN as integer ) as OTHRtot_INCN,
CAST(PA_INCN as integer ) as PA_INCN,
CAST(STATH_INCN as integer ) as STATH_INCN,
CAST(STEVH_INCN as integer ) as STEVH_INCN,
CAST(SUBStot_INCN as integer ) as SUBStot_INCN,
CAST(THFTtot_INCN as integer ) as THFTtot_INCN,
--CAPPED PAY
CAST(ELOTCap_CAPPAY as decimal(20,2) ) as ELOTCap_CAPPAY,
CAST(FICPtot_CAPPAY as decimal(20,2) ) as FICPtot_CAPPAY,
CAST(PLINCap_CAPPAY as decimal(20,2) ) as PLINCap_CAPPAY,
CAST(PLPRCap_CAPPAY as decimal(20,2) ) as PLPRCap_CAPPAY,
--CAPPED INC
CAST(ELOTCap_CAPINC as decimal(20,2) ) as ELOTCap_CAPINC,
CAST(FICPtot_CAPINC as decimal(20,2) ) as FICPtot_CAPINC,
CAST(PLINCap_CAPINC as decimal(20,2) ) as PLINCap_CAPINC,
CAST(PLPRCap_CAPINC as decimal(20,2) ) as PLPRCap_CAPINC,
--CAPPED INCN
CAST(ELOTCap_CAPINCN as integer ) as ELOTCap_CAPINCN,
CAST(FICPtot_CAPINCN as integer ) as FICPtot_CAPINCN,
CAST(PLINCap_CAPINCN as integer ) as PLINCap_CAPINCN,
CAST(PLPRCap_CAPINCN as integer )  as PLPRCap_CAPINCN,
--XS PAY
CAST(ELOTEx_XSPAY as decimal(20,2) ) as ELOTEx_XSPAY,
CAST(FIEXtot_XSPAY as decimal(20,2) ) as FIEXtot_XSPAY,
CAST(PLINEx_XSPAY as decimal(20,2) ) as PLINEx_XSPAY,
CAST(PLPREx_XSPAY as decimal(20,2) ) as PLPREx_XSPAY,
--XS INC
CAST(ELOTEx_XSINC as decimal(20,2) ) as ELOTEx_XSINC,
CAST(FIEXtot_XSINC as decimal(20,2) ) as FIEXtot_XSINC,
CAST(PLINEx_XSINC as decimal(20,2) ) as PLINEx_XSINC,
CAST(PLPREx_XSINC as decimal(20,2) ) as PLPREx_XSINC,
--XS INCN
CAST(ELOTEx_XSINCN as integer ) as ELOTEx_XSINCN,
CAST(FIEXtot_XSINCN as integer ) as FIEXtot_XSINCN,
CAST(PLINEx_XSINCN as integer ) as PLINEx_XSINCN,
CAST(PLPREx_XSINCN as integer ) as PLPREx_XSINCN--,
--TO_DATE('{{ var("max_processed_ts") }}', 'YYYY-MM-DD') AS load_date
from psicle_final_cte
