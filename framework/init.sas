/***********************************
	SAS test framework
		Init
***********************************/


%global	G_TESTROUND_ID
		G_TEST_VALUE_DATE
		G_TEST_VALUE_DT
		G_TST_LOGTABLE
		G_SINGLE_SOURCE_REL_OBSNUM
		;

;

%let	G_TESTROUND_ID=Test Fwk;			/* ID of the test round -> to be updated upon new round */
%let	G_TEST_VALUE_DATE='29FEB2016'd;		/* Value date of testing -> to be updated upon new value date to be tested */
%let	G_TST_LOGTABLE=ODS.RIDE_TST_LOG;	/* Log table */
%let	G_SINGLE_SOURCE_REL_OBSNUM=0.05;	/* Relative observation number (%) for SingleSource */

/* Calculated parameters */
%let	G_TEST_VALUE_DT=%sysfunc(dhms(&G_TEST_VALUE_DATE.,0,0,0));

