/***********************************
	SAS test framework
		Macro test_single_source
***********************************/

%macro test_single_source(
			 inDstLibname=		/* To-be-tested SAS library name */
			,inDstTblName=		/* To-be-tested table name */
			,inSrcLibname=		/* Source SAS library name */
			,inSrcTblName=		/* Source table name */
			,inKeyFields=		/* NATURAL Key fields in destination table, separated by spaces */
			,inSrcFiltering=	/* Source filtering (optional, #P_VALUE_DATE# or #P_VALUE_DT# could be used) */
			,inDstFiltering=	/* Destination filtering (optional, #P_VALUE_DATE# or #P_VALUE_DT# could be used) */
			,inSrcDstFieldMap=	/* strictly in SrcField1=DstField1 SrcField2=DstField2 ... SrcFieldn=DstFieldn format!!*/
			,inCompareDstFieldList=  /* Fields to be compared (including natural keys), separated by spaces */
			,inOutCompareTbl=	/* comparison table saved when needed. Left empty => only a temp table will be created */
			,inTestTopic=		/* test topic */
		);

%local	tmpsrclist
		tmpkfs
		tmpkftps
		tmpwhcl
		tmpexpected
		tmpactual
		tmprecnum
		tmpkeyrenam
		tmpcnt1
		tmpcnt2
		tmpfieldfrom
		tmpfieldto
		tmpfieldrenamnum
		;

%put MACRO test_single_source STARTED;
%put -------------------------------;

/* Formal param check */
/* See if any Source tables were given at all */
%if    &inSrcLibname= 
	or &inSrcTblName=  %then %do;
	%let tmpnotes=ERROR: Source table underspecified: inSrcLibname=&inSrcLibname., inSrcTblName=&inSrcTblName.! exit macro;
	%goto exitmacro;
%end;

/* See if any Destination tables were given at all */
%if    &inDstLibname= 
	or &inDstTblName=  %then %do;
	%let tmpnotes=ERROR: Source table underspecified: inDstLibname=&inDstLibname., inDstTblName=&inDstTblName.! exit macro;
	%goto exitmacro;
%end;

/* Check if key fields are provided */
%if &inKeyFields= %then %do;
	%let tmpnotes=ERROR: no key fields provided! exit macro;
	%goto exitmacro;
%end;

/* Check if fields to be compared are provided */
%if &inCompareDstFieldList= %then %do;
	%let tmpnotes=ERROR: no fields to be compared! exit macro;
	%goto exitmacro;
%end;

/* Semantic param check */
/* See if tables exist at all */
%let tmpsrclist=
	&inDstLibname..&inDstTblName.
	&inSrcLibname..&inSrcTblName.
	;
%let tmpallexist=;
%etl_test_connection( &tmpsrclist., tmpallexist);
%if ^(&tmpallexist) %then %do;
	%let tmpnotes=ERROR: connection to table {&tmpsrclist.} could not be established! exit macro;
	%goto exitmacro;
%end;

/* Init vars */
/* replace multiple space with a single one and insert commas between words */
%let tmpkfs=%sysfunc(translate( %sysfunc(compbl(&inKeyFields.)),%nrstr(,),%nrstr( ) ) );
%let tmpfrenam=%sysfunc(compbl( %sysfunc(translate( &inSrcDstFieldMap. ,%nrstr( ),%nrstr(=) ) )  ) );

/* formulate a suitable WHERE clause for the Source table when needed */
%let tmpwhclsrc=;
%if &inSrcFiltering^= %then %do;
	/* replace 
		P_VALUE_DATE with G_VALUE_DATE 
		P_VALUE_DT with G_VALUE_DT
	   if possible 		
	*/
	%let tmpwhclsrc = WHERE %sysfunc( tranwrd(&inSrcFiltering.,#P_VALUE_DATE#,%nrbquote(')%sysfunc(putn(&G_TEST_VALUE_DATE,date9.))%nrbquote(')d ) );
	%let tmpwhclsrc = %sysfunc( tranwrd(&tmpwhclsrc.,#P_VALUE_DT#,%nrbquote(')%sysfunc(putn(&G_TEST_VALUE_DATE,date9.))%nrbquote(:0:0:0')dt ) );
%end;

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

/* decompose fields to be renamed */
%let tmpcnt1 = 1;
%let tmpcnt2 = 0;
%let tmpfieldfrom=%qscan(&tmpfrenam., &tmpcnt1.);
%let tmpfieldto=%qscan(&tmpfrenam., %eval(&tmpcnt1.+1));
%do %while ("&tmpfieldfrom." ne "" and "&tmpfieldto." ne "");
	%let tmpcnt2 = %eval(&tmpcnt2. + 1);
	%let tmpfieldfrom&tmpcnt2.=&tmpfieldfrom.;
	%let tmpfieldto&tmpcnt2.=&tmpfieldto.;

	%let tmpcnt1 = %eval(&tmpcnt1. + 2);
	%let tmpfieldfrom=%qscan(&tmpfrenam., &tmpcnt1.);
	%let tmpfieldto=%qscan(&tmpfrenam., %eval(&tmpcnt1.+1));
%end;
%let tmpfieldrenamnum=&tmpcnt2.;
%put Fields to be renamed=&tmpfieldrenamnum.;

/* formulate key renaming when needed */
%let tmpkeyrenam=;
%do tmpcnt1=1 %to &tmpfieldrenamnum.;
	%let tmpfieldfrom=&&tmpfieldfrom&tmpcnt1;
	%let tmpfieldto=&&tmpfieldto&tmpcnt1;

	%if %index( %nrbquote(&tmpkfs.) , &tmpfieldto.)>0 %then %do;
		%let tmpkeyrenam= &tmpkeyrenam. &tmpfieldfrom.=&tmpfieldto.;
	%end;
%end;
%put Keys to be renamed=|&tmpkeyrenam.|;

/* Check 01: record numbers in source and target  -- Start */
%let tmpexpected=;
%let tmpactual=;
%put tmpwhclsrc=|&tmpwhclsrc.|;
%put tmpwhcldst=|&tmpwhcldst.|;
proc sql noprint;
	select count(1) format best20. into :tmpexpected
	from &inSrcLibname..&inSrcTblName.
	&tmpwhclsrc.
	;
quit;
%if(&syserr>6) %then %do;
	%let tmpnotes=ERROR: Check 01: &inSrcLibname..&inSrcTblName. could not be queried! tmpwhclsrc=|&tmpwhclsrc.|  -- exit macro;
	%goto exitmacro;
%end;
%let tmpexpected=%sysfunc(strip(&tmpexpected.));

proc sql noprint;
	select count(1) format best20. into :tmpactual
	from &inDstLibname..&inDstTblName.
	&tmpwhcldst.
	;
quit;
%if(&syserr>6) %then %do;
	%let tmpnotes=ERROR: Check 01: &inDstLibname..&inDstTblName. could not be queried! tmpwhcldst=|&tmpwhcldst.|  -- exit macro;
	%goto exitmacro;
%end;
%let tmpactual=%sysfunc(strip(&tmpactual.));

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
	TEST_TYPE = "@singleSourceRecnum";
	TESTED_FIELD = "_ALL_";
	EXPECTED_VALUE = strip("&tmpexpected.");
	ACTUAL_VALUE = strip("&tmpactual.");
	if(EXPECTED_VALUE eq ACTUAL_VALUE) then do;
		TEST_RESULT = "OK";
		TEST_RESULT_MESSAGE = "";
	end; else do;
		TEST_RESULT = "NOK";
		TEST_RESULT_MESSAGE = "Different record numbers compared to &inSrcLibname..&inSrcTblName.";
	end;

run;

/* Check 01: record numbers in source and target  -- End */

/* Check 02: select test cases  -- Start */

/* step 1: select keyset */
data work.tmpdstkeyset;
	set &inDstLibname..&inDstTblName.(
			keep=&inKeyFields.
		);
	length 	QWERTZUIOP_SELECTION_ORDER 8
			;
	if(_n_ eq 1) then call streaminit( int( datetime() ) );
	QWERTZUIOP_SELECTION_ORDER = rand("Uniform");
run;
proc sort data=work.tmpdstkeyset;
	by QWERTZUIOP_SELECTION_ORDER &inKeyFields.;
run;
proc sql noprint;
	select count(1) format best20. into :tmprecnum
	from work.tmpdstkeyset
	;
quit;
%put Dst recnum read = &tmprecnum.;
%let tmprecnum = %sysfunc( ceil( %sysevalf(&tmprecnum. * &G_SINGLE_SOURCE_REL_OBSNUM.) ) );
%put Dst recnum to be selected = &tmprecnum.;

/* step 2: build source keyset (renamed when needed) */
data work.tmpdstkeyset
     work.tmpsrckeyset
     ;
	set work.tmpdstkeyset(obs= &tmprecnum. );

	output work.tmpdstkeyset;

	%if tmpkeyrenam^= %then %do;
		rename &tmpkeyrenam.;
	%end;

	output work.tmpsrckeyset;
run;

/* step 3: select source data, sorted by KEY fields */
data work.tmpsourcedata;
	set &inSrcLibname..&inSrcTblName.(
			%if &inSrcDstFieldMap^= %then %do;
				rename=(&inSrcDstFieldMap.)
			%end;
			%if &tmpwhclsrc^= %then %do;
				where=( %sysfunc( tranwrd(&tmpwhclsrc.,WHERE,  ) ) )
			%end;
		);
	%HashJoin(work.tmpsrckeyset
				,&inKeyFields.
					,QWERTZUIOP_SELECTION_ORDER);

	if(QWERTZUIOP_SELECTION_ORDER ne .) then do;
		keep &inCompareDstFieldList.;
		output;
	end;
run;
proc sort data=work.tmpsourcedata;
	by &inKeyFields.;
run;
/* step 4: select destination data, sorted by KEY fields */
data work.tmpdestinationdata;
	set &inDstLibname..&inDstTblName.(
			%if &tmpwhclsrc^= %then %do;
				where=( %sysfunc( tranwrd(&tmpwhcldst.,WHERE,  ) ) )
			%end;
		);
	%HashJoin(work.tmpdstkeyset
				,&inKeyFields.
					,QWERTZUIOP_SELECTION_ORDER);

	if(QWERTZUIOP_SELECTION_ORDER ne .) then do;
		keep &inCompareDstFieldList.;
		output;
	end;
run;
proc sort data=work.tmpdestinationdata;
	by &inKeyFields.;
run;

/* step 5: compare data */
proc compare 	base=work.tmpsourcedata
				compare=work.tmpdestinationdata
				out=work.tmpcompareresult
				outnoequal outbase outcomp outdif noprint;
   id &inKeyFields.;
run;
proc sql noprint;
	select count(1) format best20. into :tmprecnum
	from work.tmpcompareresult
	;
run;

/* save compare results on two conditions:
	1) user wanted it
	2) there are differences
in case of only 1) holds, potential result table from a former run will be dropped
*/
%if &inOutCompareTbl^= %then %do;
	proc sql;
		drop table &inOutCompareTbl.
		;
	quit;
	%if &tmprecnum^=0 %then %do;
		data &inOutCompareTbl.;
			set work.tmpcompareresult;
		run;
	%end;
%end;

/* Check 02: select test cases  -- End */
/* book results */
proc datasets lib=work nolist nodetails; delete newrec2; run;
data work.newrec2;
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
	TEST_TYPE = "@singleSourceCompare";
	TESTED_FIELD = "_ALL_";
	EXPECTED_VALUE = strip("0");
	ACTUAL_VALUE = strip("&tmprecnum.");
	if(EXPECTED_VALUE eq ACTUAL_VALUE) then do;
		TEST_RESULT = "OK";
		TEST_RESULT_MESSAGE = "";
	end; else do;
		TEST_RESULT = "NOK";
		TEST_RESULT_MESSAGE = "Different fields in sample compared to &inSrcLibname..&inSrcTblName.";
	end;

run;

/* LOG RESULTS */
proc append	base=&G_TST_LOGTABLE.
			data=work.newrec
			force
			;
proc append	base=&G_TST_LOGTABLE.
			data=work.newrec2
			force
			;
run;


%let tmpnotes=Test completed successfully.;
%exitmacro:
%put MACRO test_single_source ENDED;
%put &tmpnotes.;
%put -----------------------------;
%mend;



/* let's test the sucker! */
/*
%put TEST: MISSING PARAMS / 1!;
%test_single_source(
			 inDstLibname=	
			,inDstTblName=	ilyen
			,inSrcLibname=	
			,inSrcTblName=	ugyse
			,inKeyFields=	egyik masik
			,inSrcFiltering=
			,inDstFiltering=
		);

%put TEST: MISSING PARAMS / 2!;
%test_single_source(
			 inDstLibname=	nincs
			,inDstTblName=	ilyen
			,inSrcLibname=	tabla
			,inSrcTblName=	ugyse
			,inKeyFields=	
			,inSrcFiltering=
			,inDstFiltering=
		);

%put TEST: table does not exist;
%test_single_source(
			 inDstLibname=	nincs
			,inDstTblName=	ilyen
			,inSrcLibname=	tabla
			,inSrcTblName=	ugyse
			,inKeyFields=	egyik masik
			,inSrcFiltering=
			,inDstFiltering=
		);

%put TEST: wrong key fields;
%test_single_source(
			 inDstLibname=	rsdm_st
			,inDstTblName=	d_branch
			,inSrcLibname=	kmdw
			,inSrcTblName=	mi_fm_branch_mth
			,inKeyFields=	egyik masik
			,inSrcFiltering=
			,inDstFiltering=
		);

%put TEST: positive;
%test_single_source(
			 inDstLibname=	rsdm_st
			,inDstTblName=	d_branch
			,inSrcLibname=	kmdw
			,inSrcTblName=	mi_fm_branch_mth
			,inKeyFields=	branch
			,inSrcFiltering=SYM_RUN_DATE=#P_VALUE_DT#
			,inDstFiltering=SYM_RUN_DATE=#P_VALUE_DATE#
		);

%put TEST: key fields renamed;
%test_single_source(
			 inDstLibname=	rsdm_st
			,inDstTblName=	d_branch
			,inSrcLibname=	kmdw
			,inSrcTblName=	mi_fm_branch_mth
			,inKeyFields=	branch bela
			,inSrcFiltering=SYM_RUN_DATE=#P_VALUE_DT#
			,inDstFiltering=SYM_RUN_DATE=#P_VALUE_DATE#
			,inSrcDstFieldMap=fiok=branch description=branch_desc_cd alma=bela
			,inTestTopic=TestTopic
		);

%test_single_source(
			 inDstLibname=	rsdm_snp
			,inDstTblName=	d_branch
			,inSrcLibname=	rsdm_st
			,inSrcTblName=	d_branch
			,inKeyFields=	branch_cd
			,inSrcFiltering=SYM_RUN_DATE=#P_VALUE_DATE#
			,inDstFiltering=VALID_FROM_DTTM<=#P_VALUE_DT#<=VALID_TO_DTTM
			,inSrcDstFieldMap=branch=branch_cd description=branch_desc
			,inCompareDstFieldList=branch_cd branch_desc
			,inTestTopic=TestTopic
		);
*/




