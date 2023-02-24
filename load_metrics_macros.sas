%macro generate_test_data(n_populations=this_n_pop, 
n_obs_per_population=this_n_obs_per_pop,
/*
	GENERATES 9 GB:
    n_populations           = 500,
    n_obs_per_population    = 500000,
*/

/* 
	GENERATES 65 GB - FAILED:
    n_populations           = 500,
    n_obs_per_population    = 3500000,
	seed					= 123
*/
	seed=
    );

	/* create test dataset */
	data work.test_data;        
		call streaminit(&seed.);
        do population_id=1 to &n_populations.;
            do iso=1 to &n_obs_per_population.;
				HCC130 = (rand('uniform')<0.02);
				HCC131 = (rand('uniform')<0.02);
				HCC132 = (rand('uniform')<0.02);
				output;
			end;
		end;
	run;


PROC SQL ;
  TITLE ‘Filesize for  Data Set’ ;
  SELECT LIBNAME,
         MEMNAME,
         FILESIZE FORMAT=SIZEKMG.,
         FILESIZE FORMAT=SIZEK.
    FROM DICTIONARY.TABLES
      WHERE LIBNAME = 'WORK'
        AND MEMNAME = 'TEST_DATA'
        AND MEMTYPE = 'DATA' ;
QUIT ;




proc sql noprint;
            select FILESIZE FORMAT=SIZEKMG.
            into :this_filesize
			/*
			separated by '" "'
			*/ 
            from DICTIONARY.TABLES
            where LIBNAME = 'WORK'
        AND MEMNAME = 'TEST_DATA'
        AND MEMTYPE = 'DATA'
		  ;
quit;


/*****************************************************************************/
/*  Terminate the specified CAS session (mySession). No reconnect is possible*/
/*****************************************************************************/
*cas mySession terminate;
cas; 
caslib _all_ assign;

/* initiate cas session */
cas mysession sessopts=(caslib="PUBLIC", timeout=3600);

	/*??? the cas libname is not needed for the code to run, but it allows us to see the library in the SAS Viya interface*/
	/*libname mycaslib cas caslib="PUBLIC";*/
	
/*MF*/
libname mycaslib cas caslib="PUBLIC";


/* we load the dataset with the groupby option to partition by population_id, 
and the orderby option to order observation within each partition by iso */

/* start a timer */
%let _timer_start = %sysfunc(datetime());

proc casutil;
	load 
		data=work.test_data
		casout="test_data"
		groupby=(population_id)
		orderby=(iso)
		replace;
quit;

/* end the timer and output the info on run time */
%let _timer_end = %sysfunc(datetime());


data _null_;
        runtime = &_timer_end. - &_timer_start;
        call symputx('_runtime', runtime);
run;

%let run_start = %left(%qsysfunc(putn(&_timer_start., DATETIME20.2)));
%let run_end = %left(%qsysfunc(putn(&_timer_end., DATETIME20.2)));
%let runtime = %left(%qsysfunc(putn(&_runtime., TIME13.2)));

%put *** LOAD Run Time for Data Step ***;
%put LOAD Start      - &run_start.;
%put LOAD End        - &run_end.;
%put LOAD Runtime    - &runtime.;

data _null_;
	   /**/
	   file "%SYSFUNC(STRIP(&this_filepath1))cas_LOAD_output_%SYSFUNC(STRIP(&this_filesize)).txt";
	   Put "LOAD Start      - &run_start.";
	   Put "LOAD End        - &run_end.";
	   /* if the value is in data step variables*/
	   put "LOAD Runtime    - &runtime.";
	   put "\n";
	   put "%SYSFUNC(STRIP(&this_filesize)), &run_start., &run_end., &runtime.";

run;



%mend;


%macro example_spre();

    /* start a timer */
    %let _timer_start = %sysfunc(datetime());

	/* we take the test_data through a data step to subset to top-300 observations for each population 
	and for the top-400000 that meet a certain condition (and were not in the original top-100)*/
    data 
        work.tier1(drop=tier2_elig_flag) 
        work.tier_2_cohort(drop=tier1_flag tier);
        set work.test_data;
        by population_id iso;

        /* count people in each population to select */
        retain in_population_counter 0;
        if first.population_id then in_population_counter = 1;
        else in_population_counter = in_population_counter + 1;

        /* first, output top-100 observations */
        if in_population_counter<=300 then do;
            tier1_flag = 1;
            tier = 1;
            output work.tier1;
        end;

        /* next, output observations within top-490300 and meeting the condition description */
        else if in_population_counter<=490300 then do;
            if (HCC130 = 1 or HCC131 = 1 or HCC132 = 1) then do;
                cohort = 1;
                tier2_elig_flag = 1;
                output work.tier_2_cohort;
            end;
        end;
	run;

    /* end the timer and output the info on run time */
    %let _timer_end = %sysfunc(datetime());

    data _null_;
        runtime = &_timer_end. - &_timer_start;
        call symputx('_runtime', runtime);
    run;

    %let run_start = %left(%qsysfunc(putn(&_timer_start., DATETIME20.2)));
    %let run_end = %left(%qsysfunc(putn(&_timer_end., DATETIME20.2)));
    %let runtime = %left(%qsysfunc(putn(&_runtime., TIME13.2)));

    %put *** Run Time for Data Step ***;
    %put Start      - &run_start.;
    %put End        - &run_end.;
    %put Runtime    - &runtime.;
/*
    %let this_filesize2=%SYSFUNC(STRIP(&this_filesize))
	file "/cisfelts-export/sseviya/homes/Manuel.Figallo@sas.com/cas_output_%SYSFUNC(STRIP(&this_filesize)).txt";
	
	%let this_filepath0=/tmp/;
	%let this_filepath1=/cisfelts-export/sseviya/homes/Manuel.Figallo@sas.com/;
	
*/

	data _null_;
	   /**/
	   file "%SYSFUNC(STRIP(&this_filepath1))spre_output_%SYSFUNC(STRIP(&this_filesize)).txt";
	   Put "Start      - &run_start.";
	   Put "End        - &run_end.";
	   /* if the value is in data step variables*/
	   put "Runtime    - &runtime.";
	   put "\n";
	   put "%SYSFUNC(STRIP(&this_filesize)), &run_start., &run_end., &runtime.";

	run;


%mend;


%macro example_cas();
	
    /* start a timer */
    %let _timer_start = %sysfunc(datetime());


	/* we take the test_data through a data step to subset to top-300 observations for each population 
	and for the top-400000 that meet a certain condition (and were not in the original top-100)*/
    data 
        mycaslib.tier1(drop=tier2_elig_flag) 
        mycaslib.tier_2_cohort(drop=tier1_flag tier);
        set mycaslib.test_data;
        by population_id iso;

        /* count people in each population to select */
        retain in_population_counter 0;
        if first.population_id then in_population_counter = 1;
        else in_population_counter = in_population_counter + 1;

        /* first, output top-100 observations */
        if in_population_counter<=300 then do;
            tier1_flag = 1;
            tier = 1;
            output mycaslib.tier1;
        end;

        /* next, output observations within top-490300 and meeting the condition description */
        else if in_population_counter<=490300 then do;
            if (HCC130 = 1 or HCC131 = 1 or HCC132 = 1) then do;
                cohort = 1;
                tier2_elig_flag = 1;
                output mycaslib.tier_2_cohort;
            end;
        end;
	run;

    /* end the timer and output the info on run time */
    %let _timer_end = %sysfunc(datetime());

    data _null_;
        runtime = &_timer_end. - &_timer_start;
        call symputx('_runtime', runtime);
    run;

    %let run_start = %left(%qsysfunc(putn(&_timer_start., DATETIME20.2)));
    %let run_end = %left(%qsysfunc(putn(&_timer_end., DATETIME20.2)));
    %let runtime = %left(%qsysfunc(putn(&_runtime., TIME13.2)));

    %put *** Run Time for Data Step ***;
    %put Start      - &run_start.;
    %put End        - &run_end.;
    %put Runtime    - &runtime.;

	data _null_;
	   /*file "/tmp/outputtest.txt";*/
	   file "%SYSFUNC(STRIP(&this_filepath1))cas_output_%SYSFUNC(STRIP(&this_filesize)).txt";
	   Put "Start      - &run_start.";
	   Put "End        - &run_end.";
	   /* if the value is in data step variables*/
	   put "Runtime    - &runtime.";
	   put "\n";
	   put "%SYSFUNC(STRIP(&this_filesize)), &run_start., &run_end., &runtime.";

	run;


%mend;



%macro example_spre_sql();

    /* start a timer */
    %let _timer_start = %sysfunc(datetime());

	/* we take the test_data through a data step to subset to top-300 observations for each population 
	and for the top-400000 that meet a certain condition (and were not in the original top-100)*/
	PROC SQL;
	 SELECT SUM(HCC130), SUM(HCC131), SUM(HCC132)
	 FROM work.test_data;
	QUIT;

    /* end the timer and output the info on run time */
    %let _timer_end = %sysfunc(datetime());

    data _null_;
        runtime = &_timer_end. - &_timer_start;
        call symputx('_runtime', runtime);
    run;

    %let run_start = %left(%qsysfunc(putn(&_timer_start., DATETIME20.2)));
    %let run_end = %left(%qsysfunc(putn(&_timer_end., DATETIME20.2)));
    %let runtime = %left(%qsysfunc(putn(&_runtime., TIME13.2)));

    %put *** Run Time for Data Step ***;
    %put Start      - &run_start.;
    %put End        - &run_end.;
    %put Runtime    - &runtime.;
/*
    %let this_filesize2=%SYSFUNC(STRIP(&this_filesize))
*/
	data _null_;
	   /*file "/tmp/outputtest.txt";*/
	   file "%SYSFUNC(STRIP(&this_filepath1))spre_sql_output_%SYSFUNC(STRIP(&this_filesize)).txt";
	   Put "Start      - &run_start.";
	   Put "End        - &run_end.";
	   /* if the value is in data step variables*/
	   put "Runtime    - &runtime.";
	   put "\n";
	   put "%SYSFUNC(STRIP(&this_filesize)), &run_start., &run_end., &runtime.";

	run;
%mend;



%macro example_cas_sql();
    /* start a timer */
    %let _timer_start = %sysfunc(datetime());

/*
	libname test1 cas caslib="PUBLIC";	
	caslib _all_ assign sessref=mysession;
  	caslib _all_ list;
*/
	/* we take the test_data through a data step to subset to top-300 observations for each population 
	and for the top-400000 that meet a certain condition (and were not in the original top-100)*/
/**/
	PROC FEDSQL sessref=mysession;
	 select sum(HCC130), sum(HCC131), sum(HCC132)
	 from PUBLIC.test_data;
	QUIT;

    /* end the timer and output the info on run time */
    %let _timer_end = %sysfunc(datetime());

    data _null_;
        runtime = &_timer_end. - &_timer_start;
        call symputx('_runtime', runtime);
    run;

    %let run_start = %left(%qsysfunc(putn(&_timer_start., DATETIME20.2)));
    %let run_end = %left(%qsysfunc(putn(&_timer_end., DATETIME20.2)));
    %let runtime = %left(%qsysfunc(putn(&_runtime., TIME13.2)));

    %put *** Run Time for Data Step ***;
    %put Start      - &run_start.;
    %put End        - &run_end.;
    %put Runtime    - &runtime.;
/*
    %let this_filesize2=%SYSFUNC(STRIP(&this_filesize))
*/
	data _null_;
	   /*file "/tmp/outputtest.txt";*/
	   file "%SYSFUNC(STRIP(&this_filepath1))cas_sql_output_%SYSFUNC(STRIP(&this_filesize)).txt";
	   Put "Start      - &run_start.";
	   Put "End        - &run_end.";
	   /* if the value is in data step variables*/
	   put "Runtime    - &runtime.";
	   put "\n";
	   put "%SYSFUNC(STRIP(&this_filesize)), &run_start., &run_end., &runtime.";

	run;
%mend;



%macro example_cas_sql_test1();
 	*libname test1 cas caslib="PUBLIC";
	*libname mycaslib cas caslib="PUBLIC";

	caslib _all_ assign sessref=mysession;
  	caslib _all_ list;

	proc fedsql sessref=mysession;
	 select sum(HCC130)
	 from PUBLIC.test_data;
	quit;

%mend;


%macro copyFS2Viya(this_src=, this_dest=);
	filename src &this_src;
	filename dest filesrvc folderpath="/Public" filename=&this_dest debug=http;
	data _null_;
		rc=fcopy("src","dest");
		msg=sysmsg();
		put rc=;
		put msg=;
	run;
%mend;