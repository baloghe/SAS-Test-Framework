/***********************************
	SAS test framework
		Macro test_fillup_percent
***********************************/

%macro test_fillup_percent(
			 inDstLibname=			/* To-be-tested SAS library name */
			,inDstTblName=			/* To-be-tested table name */
			,inDstFiltering=		/* Destination filtering (optional, #P_VALUE_DATE# or #P_VALUE_DT# could be used) */
			,inFieldToBeChecked=	/* Name of field to be checked */
			,inNonMissLowerThrs=	/* Relative lower threshold for expected fillup percent */
			,inTestTopic=			/* test topic */
		);

%local	tmpwhclsrc
		tmpsrclist
		tmpallexist
		tmpnotes
		recnum
		recnum2
		tmpactual
		;

%put MACRO test_fillup_percent STARTED;
%put -------------------------------;

/* Formal param check */
/* See if any Destination tables were given at all */
%if    &inDstLibname= 
	or &inDstTblName=  %then %do;
	%let tmpnotes=ERROR: Source table underspecified: inDstLibname=&inDstLibname., inDstTblName=&inDstTblName.! exit macro;
	%goto exitmacro;
%end;

/* See if a to-be-checked field has been provided at all */
%if &inFieldToBeChecked=  %then %do;
	%let tmpnotes=ERROR: Field to be checked missing: inFieldToBeChecked=&inFieldToBeChecked.! exit macro;
	%goto exitmacro;
%end;

/* Semantic param check */
/* See if tables exist at all */
%let tmpsrclist=
	&inDstLibname..&inDstTblName.
	;
%let tmpallexist=;
%etl_test_connection( &tmpsrclist., tmpallexist);
%if ^(&tmpallexist) %then %do;
	%let tmpnotes=ERROR: connection to table {&tmpsrclist.} could not be established! exit macro;
	%goto exitmacro;
%end;

/* Init vars */
/* formulate a suitable WHERE clause for the Target table when needed */
%let tmpwhcldst=;
%if &inDstFiltering^= %then %do;
	/* replace 
		P_VALUE_DATE with G_VALUE_DATE 
		P_VALUE_DT with G_VALUE_DT
	   if possible 		
	*/
	%let tmpwhcldst = WHERE %sysfunc( tranwrd(&inDstFiltering.,#P_VALUE_DATE#,%nrbquote(')%sysfunc(putn(&G_TEST_VALUE_DATE,date9.))%nrbquote(')d ) );
	%let tmpwhcldst = %sysfunc( tranwrd(&tmpwhcldst.,#P_VALUE_DT#,%nrbquote(')%sysfunc(putn(&G_TEST_VALUE_DATE,date9.))%nrbquote(:0:0:0')dt ) );
%end;

/* Check 01: record numbers in target  -- Start */
proc sql noprint;
	sysecho "count recnum for &inDstLibname..&inDstTblName.";
	select count(1) format best20. into :recnum
	from &inDstLibname..&inDstTblName.
	&tmpwhcldst.
	;
quit;
/* Check 01: record numbers in target  -- End */

/* Check 02: number of non-missing values of required field in target  -- Start */
proc sql noprint;
	sysecho "count missing [&inFieldToBeChecked.] for &inDstLibname..&inDstTblName.";
	select count(1) format best20. into :recnum2
	from &inDstLibname..&inDstTblName.
	&tmpwhcldst.
	%if &tmpwhcldst= %then %do;
		WHERE &inFieldToBeChecked. is missing
	%end; %else %do;
		AND &inFieldToBeChecked. is missing
	%end;
	;
quit;
%if(&syserr>6) %then %do;
	%let tmpnotes=ERROR: Check 02: &inDstLibname..&inDstTblName. could not be queried for missing &inFieldToBeChecked. values! exit macro;
	%goto exitmacro;
%end;
%if &recnum^=0 %then %do;
	%let tmpactual = %sysevalf( 1.0 - %sysevalf(&recnum2. / &recnum.) );
%end; %else %do;
	%let tmpactual = .;
%end;
/* Check 02: number of non-missing values of required field in target  -- End */

/* book results */
proc datasets lib=work nolist nodetails; delete newrec; run;
data work.newrec;
	length	ETL_LOAD_DATE 8
			LIBRARY_NAME $8
			TABLE_NAME $32
			VALUE_DATE 8
			TEST_ROUND_ID $10
			TEST_TOPIC $100
			TEST_TYPE $50
			TESTED_FIELD $32
			TEST_RESULT $5
			EXPECTED_VALUE $100
			ACTUAL_VALUE $100
			TEST_RESULT_MESSAGE $200
			;

	format	ETL_LOAD_DATE	datetime22.
			VALUE_DATE		yymmdd10.
			;

	ETL_LOAD_DATE = datetime();
	LIBRARY_NAME = "&inDstLibname.";
	TABLE_NAME = "&inDstTblName.";
	VALUE_DATE = &G_TEST_VALUE_DATE.;
	TEST_ROUND_ID = "&G_TESTROUND_ID.";
	TEST_TOPIC = "&inTestTopic.";
	TEST_TYPE = "@fillup";
	TESTED_FIELD = "&inFieldToBeChecked.";
	EXPECTED_VALUE = strip("&inNonMissLowerThrs.");
	ACTUAL_VALUE = strip("&tmpactual.");
	if(EXPECTED_VALUE le ACTUAL_VALUE) then do;
		TEST_RESULT = "OK";
		TEST_RESULT_MESSAGE = "";
	end; else do;
		TEST_RESULT = "NOK";
		TEST_RESULT_MESSAGE = "Minimum fillup % (&inNonMissLowerThrs.) not reached";
	end;

run;

/* LOG RESULTS */

proc append	base=&G_TST_LOGTABLE.
			data=work.newrec
			force
			;
run;


%let tmpnotes=Test completed successfully.;
%exitmacro:
%put MACRO test_fillup_percent ENDED;
%put &tmpnotes.;
%put -----------------------------;
%mend test_fillup_percent;

/*
%put Test numeric field fillup ratio - Positive;
%test_fillup_percent(
			 inDstLibname=rsdm_snp
			,inDstTblName=F_ACCOUNT
			,inDstFiltering=VALID_DTTM eq #P_VALUE_DT#
			,inFieldToBeChecked=IFRS_LGD_RT
			,inNonMissLowerThrs=0.9
			,inTestTopic=TestTheTest
		);

%put Test numeric field fillup ratio - Negative;
%test_fillup_percent(
			 inDstLibname=rsdm_snp
			,inDstTblName=F_ACCOUNT
			,inDstFiltering=VALID_DTTM eq #P_VALUE_DT#
			,inFieldToBeChecked=IFRS_LGD_RT
			,inNonMissLowerThrs=0.99
			,inTestTopic=TestTheTest
		);
*/

