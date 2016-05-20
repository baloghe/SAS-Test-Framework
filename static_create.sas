/***********************************
	SAS test framework
		Static create tables
***********************************/

%let dstlib=ods;

/* Log table  -- Start */
proc sql;
	create table &dstlib..RIDE_TST_LOG (

		 ETL_LOAD_DATE numeric format datetime20. not null
		,LIBRARY_NAME varchar(8) not null
		,TABLE_NAME varchar(32) not null
		,VALUE_DATE numeric format yymmdd10. not null
		,TEST_ROUND_ID varchar(10) not null
		,TEST_TOPIC varchar(100)
		,TEST_TYPE varchar(50) not null
		,TESTED_FIELD varchar(32) not null
		,TEST_RESULT varchar(5) not null
		,EXPECTED_VALUE varchar(100) 
		,ACTUAL_VALUE varchar(100) 
		,TEST_RESULT_MESSAGE varchar(200) 

	);
quit;

proc datasets lib=&dstlib. nolist nodetails;
	modify RIDE_TST_LOG;
		index create pk=(	LIBRARY_NAME
							TABLE_NAME
							VALUE_DATE
							TEST_ROUND_ID
							TEST_TYPE
							TESTED_FIELD
							);
	run;
run;
/* Log table  -- End */



/* Single source  -- Start */
proc sql;
	create table &dstlib..RIDE_TST_SINGLE_SOURCE (

		 DEST_LIBRARY_NAME varchar(8) not null
		,DEST_TABLE_NAME varchar(32) not null
		,SOURCE_LIBRARY_NAME varchar(8) not null
		,SOURCE_TABLE_NAME varchar(32) not null
		,DEST_KEY_FIELDS varchar(200) not null
		,SOURCE_FILTERING varchar(200)
		,SOURCE_TO_DEST_FIELDMAP varchar(300)
		,COMPARE_DEST_FIELDLIST varchar(3000)
		,OUT_COMPARE_RESULTS_TBL varchar(41)
		,TEST_TOPIC varchar(100)

	);
quit;


proc datasets lib=&dstlib. nolist nodetails;
	modify RIDE_TST_SINGLE_SOURCE;
		index create pk=(	DEST_LIBRARY_NAME
							DEST_TABLE_NAME
							);
	run;
run;
/* Single source  -- End */


