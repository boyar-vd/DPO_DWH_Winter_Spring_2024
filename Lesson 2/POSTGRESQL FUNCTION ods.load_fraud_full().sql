-- DROP FUNCTION ods.load_fraud_full();

CREATE OR REPLACE FUNCTION ods.load_fraud_full()
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'pg_temp'
AS $function$
	
declare
	v_current_date timestamp;

begin

	v_current_date:=now();
	
	TRUNCATE TABLE ods.fraud_data;
	
	INSERT INTO ods.fraud_data 
	("Unnamed: 0"
	,trans_date_trans_time
	,cc_num
	,merchant
	,category
	,amt
	,"first"
	,"last"
	,gender
	,street
	,city
	,state
	,zip
	,lat
	,long
	,city_pop
	,job
	,dob
	,trans_num
	,unix_time
	,merch_lat
	,merch_long
	,is_fraud
	,updated_at)
		SELECT "Unnamed: 0"
				,trans_date_trans_time::timestamp
				,cc_num
				,merchant
				,category
				,amt
				,"first"
				,"last"
				,gender
				,street
				,city
				,state
				,zip
				,lat
				,long
				,city_pop
				,job
				,dob::date
				,trans_num
				,unix_time
				,merch_lat
				,merch_long
				,is_fraud
				,v_current_date
			FROM stg.csv_fraud_data
			;
end;

$function$
;