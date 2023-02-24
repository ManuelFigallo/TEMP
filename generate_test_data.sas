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