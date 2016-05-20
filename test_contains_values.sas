/***********************************
	SAS test framework
		Macro test_contains_values
***********************************/

%macro test_contains_values(
			 inDstLibname=			/* To-be-tested SAS library name */
			,inDstTblName=			/* To-be-tested table name */
			,inDstFiltering=		/* Destination filtering (optional, #P_VALUE_DATE# or #P_VALUE_DT# could be used) */
			,inFieldToBeChecked=	/* Name of field to be checked */
			,inExpectedValues=		/* Space-separated enumeration of values to be contained, quotation not needed */
			,inTestTopic=			/* test topic */
		);

%local	tmpwhclsrc
		tmpsrclist
		tmpallexist
		tmpnotes
		tmpactual
		tmpfieldtype
		tmpfieldlen
		tmpvalueset
		tmpmissvalues
		tmpcnt1
		tmpvaluenum
		tmpvalue
		;

%put MACRO test_contains_values STARTED;
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

/* See if a expected values have been provided at all */
%if &inExpectedValues=  %then %do;
	%let tmpnotes=ERROR: Expected values for field [&inFieldToBeChecked.] are missing: inExpectedValues=&inExpectedValues.! exit macro;
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

/* determine field type */
%let tmpfieldtype=;
data _null_;
	set &inDstLibname..&inDstTblName.(
			keep=&inFieldToBeChecked.
			obs=1
		);
	call symput ( "tmpfieldtype" , vtype( &inFieldToBeChecked. ) );
	call symput ( "tmpfieldlen"  , vlength( &inFieldToBeChecked. ) );
run;
%if(&syserr>6) %then %do;
	%let tmpnotes=ERROR: determine field type: first record of &inDstLibname..&inDstTblName. could not be queried for &inFieldToBeChecked. field! exit macro;
	%goto exitmacro;
%end;

/* input expected values into a data set */
%let tmpvalueset=%sysfunc( strip ( %sysfunc( compbl( &inExpectedValues. ) ) ) );
%let tmpcnt1 = 1;
%let tmpvalue=%scan(&tmpvalueset., &tmpcnt1., %str( ));
%do %while ("&tmpvalue." ne "");
	%let tmpvalue&tmpcnt1.=&tmpvalue.;

	%let tmpcnt1 = %eval(&tmpcnt1. + 1);
	%let tmpvalue=%scan(&tmpvalueset., &tmpcnt1., %str( ));
%end;
%let tmpvaluenum=%eval(&tmpcnt1.-1);
data work.tmpvalues;
	length &inFieldToBeChecked.
			%if &tmpfieldtype=C %then %do; $ %end;
			&tmpfieldlen.
			;

	%do tmpcnt1=1 %to &tmpvaluenum.;
		%if &tmpfieldtype=C %then %do;
			&inFieldToBeChecked. = "&&tmpvalue&tmpcnt1."; output;
		%end; %else %do;
			&inFieldToBeChecked. = &&tmpvalue&tmpcnt1.; output;
		%end;
	%end;
run;

/* Check 01: look for nonexisting but expected values  -- Start */
proc sql noprint;
	sysecho "Look for expected values in [&inFieldToBeChecked.] in &inDstLibname..&inDstTblName.";
	select distinct &inFieldToBeChecked. into :tmpmissvalues separated by " "
	from (
		select q.&inFieldToBeChecked.
		      ,x.recnum
		from work.tmpvalues q
		left join (
			select v.&inFieldToBeChecked.
			      ,count(1) as recnum
			from work.tmpvalues v
			inner join &inDstLibname..&inDstTblName.(
							%if &tmpwhclsrc^= %then %do;
								where=( %sysfunc( tranwrd(&tmpwhcldst.,WHERE,  ) ) )
							%end;
						) t
				on v.&inFieldToBeChecked. eq t.&inFieldToBeChecked.
			group by v.&inFieldToBeChecked.
		) x
			on q.&inFieldToBeChecked. eq x.&inFieldToBeChecked.
		having x.recnum eq .
	)
	;
quit;
%put Missing but expected values=|&tmpmissvalues.|;
/* Check 01: look for nonexisting but expected values  -- End */

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
	TEST_TYPE = "@contains(values)";
	TESTED_FIELD = "&inFieldToBeChecked.";
	EXPECTED_VALUE = strip("");
	ACTUAL_VALUE = strip("&tmpmissvalues.");
	if(EXPECTED_VALUE eq ACTUAL_VALUE) then do;
		TEST_RESULT = "OK";
		TEST_RESULT_MESSAGE = "";
	end; else do;
		TEST_RESULT = "NOK";
		TEST_RESULT_MESSAGE = "Missing values {&tmpmissvalues.}";
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
%put MACRO test_contains_values ENDED;
%put &tmpnotes.;
%put -----------------------------;
%mend test_contains_values;

/*
%put Test numeric field fillup ratio - Char, Positive;
%test_contains_values(
			 inDstLibname=rsdm_snp
			,inDstTblName=F_ACCOUNT
			,inDstFiltering=VALID_DTTM eq #P_VALUE_DT#
			,inFieldToBeChecked=ENTITY_CD
			,inExpectedValues=EBH
			,inTestTopic=TestTheTest
		);

%put Test numeric field fillup ratio - Char, Negative;
%test_contains_values(
			 inDstLibname=rsdm_snp
			,inDstTblName=F_ACCOUNT
			,inDstFiltering=VALID_DTTM eq #P_VALUE_DT#
			,inFieldToBeChecked=ENTITY_CD
			,inExpectedValues=ABC
			,inTestTopic=TestTheTest
		);


%put Test numeric field fillup ratio - Num, Negative;
%test_contains_values(
			 inDstLibname=rsdm_snp
			,inDstTblName=F_ACCOUNT
			,inDstFiltering=VALID_DTTM eq #P_VALUE_DT#
			,inFieldToBeChecked=IFRS_LGD_RT
			,inExpectedValues=0.9 2  4e9 '30SEP2015'd
			,inTestTopic=TestTheTest
		);
*/


