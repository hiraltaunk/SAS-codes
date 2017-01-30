*Task2---Importing the files timesData.csv, shanghaiData.csv and cwurData.csv into the SAS environment;

data WORK.timesData                               ;
       %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
       infile 'Z:\SAS_SharedFolder\timesData.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
          informat world_rank $10. ;
          informat university_name $73. ;
          informat country $24. ;
          informat teaching best32. ;
          informat international $5. ;
          informat research best32. ;
          informat citations best32. ;
          informat income $7. ;
          informat total_score $10. ;
          informat num_students $8. ;
          informat student_staff_ratio best32. ;
          informat international_students percent32. ;
          informat female_male_ratio $9. ;
          informat year best32. ;
          format world_rank $10. ;
          format university_name $73. ;
          format country $24. ;
          format teaching best12. ;
          format international $5. ;
          format research best12. ;
          format citations best12. ;
          format income $7. ;
          format total_score $10. ;
          format num_students $8. ;
          format student_staff_ratio best12. ;
          format international_students percent12. ;
          format female_male_ratio $9. ;
          format year best12. ;
       input
                   world_rank $
                   university_name $
                  country $
                   teaching
                   international $
                   research
                   citations
                   income $
                   total_score $
                   num_students $
                   student_staff_ratio
                   international_students
                   female_male_ratio $
                   year
       ;
       if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;

data WORK.shanghaiData                            ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile 'Z:\SAS_SharedFolder\shanghaiData.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
         informat world_rank $10. ;
         informat university_name $51. ;
         informat national_rank $10. ;
         informat total_score best32. ;
         informat alumni best32. ;
         informat award best32. ;
         informat hici best32. ;
         informat ns best32. ;
         informat pub best32. ;
         informat pcp best32. ;
         informat year best32. ;
         format world_rank $10. ;
         format university_name $51. ;
         format national_rank $10. ;
         format total_score best12. ;
         format alumni best12. ;
         format award best12. ;
         format hici best12. ;
         format ns best12. ;
         format pub best12. ;
         format pcp best12. ;
         format year best12. ;
      input
                  world_rank $
                  university_name $
                  national_rank $
                  total_score
                  alumni
                  award
                  hici
                  ns
                  pub
                  pcp
                  year
     ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;
  data WD.cwurdata                                  ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile 'Z:\SAS_SharedFolder\cwurData.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
         informat world_rank best32. ;
         informat institution $59. ;
         informat country $18. ;
         informat national_rank best32. ;
         informat quality_of_education best32. ;
         informat alumni_employment best32. ;
         informat quality_of_faculty best32. ;
         informat publications best32. ;
         informat influence best32. ;
         informat citations best32. ;
         informat broad_impact $1. ;
         informat patents best32. ;
         informat score best32. ;
         informat year best32. ;
         format world_rank best12. ;
         format institution $59. ;
         format country $18. ;
         format national_rank best12. ;
         format quality_of_education best12. ;
         format alumni_employment best12. ;
         format quality_of_faculty best12. ;
         format publications best12. ;
         format influence best12. ;
         format citations best12. ;
         format broad_impact $1. ;
         format patents best12. ;
         format score best12. ;
         format year best12. ;
      input
                  world_rank
                  institution $
                  country $
                  national_rank
                  quality_of_education
                  alumni_employment
                  quality_of_faculty
                  publications
                  influence
                  citations
                  broad_impact $
                  patents
                 score
                  year
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;

libname wd 'Z:\SAS_SharedFolder';

data wd.timesdata;
set work.timesdata;
run;

data wd.shanghaidata;
set work.shanghaidata;
run;

* Section 1
Q0: Ensure all score related features (eg. teaching, international etc) are numeric and have '.' representing missing values in them. ;

data wd.timesdata;
set wd.timesdata;
newinternational=input(international,best32.);
drop international;
rename newinternational=international;
run;

data wd.timesdata;
set wd.timesdata;
newincome=input(income,best32.);
drop income;
rename newincome=income;
run;

data wd.timesdata;
set wd.timesdata;
new_total_score=input(total_score,best32.);
drop total_score;
rename new_total_score=total_score;
run;

data wd.timesdata;
set wd.timesdata;
new_world_rank=input(world_rank,best32.);
drop world_rank;
rename new_world_rank=world_rank;
run;

  proc contents data=wd.timesdata;
  run;

*Remove the percentage sign in the field "international_students" and convert the numeric part to its corresponding fraction;
data wd.timesdata;
set wd.timesdata;
new_international_students=scan(international_students,1,'%');
drop international_students;
run;
data wd.timesdata;
set wd.timesdata;
international_students=new_international_students;
drop new_international_students;
run;

*Derive two fields (female_proportion and male_proportion) from female_male_ratio field. ;
data wd.timesdata;
set wd.timesdata;
female_proportion=scan(female_male_ratio,1,':');
male_proportion=scan(female_male_ratio,2,':');
run;

*Q1: Create a subset of the timesData which features only top 10 universities in the world ranking for all represented years. ;
data wd.timesdata_top10_universities;
set wd.timesdata;
where world_rank <=10;
run;

proc sort data=	wd.timesdata_top10_universities out=wd.top_100_university_byyear;
by year;
run;

* Q2: Create a subset of the timesData which features only universities from the United States of America. ;
data wd.timesdata_USA;
set wd.timesdata;
where country='United States of America';
run;

*Q3: Find the largest universities in terms of "number of students" for each country and year represented in the dataset.;
proc sort data=wd.timesdata out=wd.timesdata_largest_univ;
by country year descending num_students ;
run; 

data wd.larg_univ; 
set wd.timesdata_largest_univ;
by country year descending num_students ;
if  first.year then output ;
run; 


*Q4: Find the highest ranking Indian university/universities for the year 2016.;

data wd.year_2016_country_india;
set wd.timesdata;
where year=2016 and country='India';
run;

proc sort data=wd.year_2016_country_india out=wd.highest_ranking;
by descending world_rank;
run;
proc rank data=wd.highest_ranking out= wd.highest_ranking_university ties=dense;
var world_rank;
ranks rank_1
run;

data wd.highest_ranking_university ;
set wd.highest_ranking_university ;
where rank_1=1;
run;


*Q5: Find the universities which have featured in the top 25 of the world ranking more 3 times.;
*selecting top 25 universities;
data wd.top_25_univ_ranking; 
set wd.timesdata;
where world_rank <=25;
keep university_name;
run;

proc sort data=wd.top_25_univ_ranking out=wd.top_25_univ;
by university_name;
run;

*counting the universities;
data wd.top_25_univ_count;
  set wd.top_25_univ;
  by university_name;
  if first.university_name then count=0;
  count+1;
  if last.university_name then output;
run;

*selecting top 25 universities;
data wd.top_25_univ_count;
  set wd.top_25_univ_count;
  if count>3;
  keep university_name;
  run;

*Q6: Create a dataset which contains each represented country and its frequency (consider only top 100 ranks) in the timesData dataset. ;

data wd.top100_universities;
set wd.timesdata;
where world_rank<101 ;
run;

proc freq  data=wd.top100_universities;
table country /out=wd.university_freq;
run;

* section2 ;


*Q0: Create a university-country map from the "timesData" dataset. The new dataset should be deduped on the university column. ;

data wd.shanghaidata;
set work.shanghaidata; 
run;

data wd.timesdata;
set wd.timesdata;
university=catx('-',university_name,country) ;
run;


*Q1: Add country information to "shanghaiData" dataset by making use of the newly created "university-country map" dataset (from the question Q0).;


data wd.timesdata_country;
set wd.timesdata;
country=scan(university,2,'-');
run;

data wd.shanghaidata;
merge wd.shanghaidata wd.timesdata_country;
by university_name;
run;


*Q2: Add two extra columns to the "shanghaiData" dataset, the first column giving the previous year's total score for the university and the second one giving the average total score.;
*second one giving th average total score.;
proc means data=wd.shanghaidata;
var total_score;
by university_name;
output out=wd.shanghaidata3 mean= average_total_score;
run;

proc sql;
create table wd.shanghai_1 as 
select a.* ,b.average_total_score from wd.shanghaidata as a left join wd.shanghaidata3 as b on a.university_name=b.university_name;
run;

*the first column giving the previous year's total score for the university;
proc sql ;
create table wd.shanghaidata as
select u.world_rank,u.university_name,u.national_rank,u1.total_score,u.alumni,u.award,u.hici,u.ns,u.pub,u.pcp,u.year,u.avg_total_score from wd.shanghaiData u left join wd.shanghaiData u1 on u.university_name=u1.university_name and (u.year-1)=(u1.year);
quit;
run;

data wd.shanghaidata;
set wd.shanghaidata;
rename total_score=total_score_previous;
run;

proc sql ;
create table wd.shanghaidata as
select u.*,u1.total_score from wd.shanghaidata u left join work.shanghaidata u1 on u.university_name=u1.university_name and u.year=u1.year;
run;


*Q3: Create a new dataset with university name, country & average total score of university (use the final dataset from question Q2). 
The new dataset should be deduped on the university column and then rank the universities based on their average total scores. 
University with the highest average total score will be ranked 1st.	;

data wd.shanghaidata_rank;
set wd.shanghaidata;
keep university_name country avg_total_score ;
run;

proc sort  data=wd.shanghaidata_rank out=wd.shanghaidata_rank;
by avg_total_score;
run;

proc rank data=wd.shanghaidata_rank out=wd.shanghaidata_rank_score descending ties=dense;
var avg_total_score	;
ranks Rank;
run;

proc sort  data=wd.shanghaidata_rank_score out=wd.shanghaidata_rank_score;
by Rank ;
run;

*Q4: Merge the new average score based ranking (from the question Q3) to the original "shanghaiData" dataset.;

data wd.shanghaidata;
merge wd.shanghaidata wd.shanghaidata_rank_score;
by university_name;
run;

*Q5: Create a new dataset with university name as the id column and the total score for different years represented as individual columns. (Transpose the  "shanghaiData" dataset);

data wd.shanghai_new;
set wd.shanghaidata;
keep university_name year total_score;
run;

proc sort data=wd.shanghai_new out=wd.shanghai_new;
by university_name;
run;

data wd.shanghai_new ;set wd.shanghai_new;
by university name year;
if first.year;
run;

proc transpose data=wd.shanghai_new out=wd.shanghai_transpose;
by university_name;
id year;
var total_score;
run;

*Q6: Create a new dataset with the following columns - university_name, first_year, latest_year, change_rank 
where first_year would represent the first time a university got represented in the "shanghaiData" dataset 
and latest_year the latest time, change_rank would represent the change in rank between the first and the latest;

proc sort data= wd.shanghaidata out= wd.shanghaidata_first_last;
by university_name;
run;

data wd.shanghaidata_year;
set wd.shanghaidata_first_last;
by  university_name year;
if first.university_name then flag=1;
else if last.university_name then flag=0;
else flag=2;
run;

data wd.shanghaidata_first_year;
set wd.shanghaidata_year;
keep university_name year world_rank ;
where flag=1 ;
run;

data wd.shanghaidata_last_year;
set wd.shanghaidata_year;
keep university_name year world_rank ;
where flag=0 ;
run;

proc sql;
create table wd.shanghaidata_data as 
select a.university_name,a.year as first_time,b.year as last_time,(a.world_rank-b.world_rank )as change_rank from wd.shanghaidata_first_year as a left join wd.shanghaidata_last_year as b on a.university_name=b.university_name; 
quit;
run;

*Section3;
*Q0: Create a new dataset with total scores from all three datasets represented as individual columns. 
Each record would have a university name, a year and the corresponding total scores from all three datasets. 
Consider years which are common across datasets (2012 to 2015). ;

data wd.timesdata_1;
set wd.timesdata;
keep university_name year total_score;
where year in (2012,2013,2014,2015);
run;

data wd.shanghaidata_1;
set  wd.shanghaidata;
keep university_name year total_score;
where year in (2012,2013,2014,2015);
run;

data wd.cwurdata_1;
set  wd.cwurdata;
keep institution year score;
where year in (2012,2013,2014,2015);
run;

proc sql;
create table wd.total_score_2dataset as 
select a.university_name ,a.year,a.total_score as total_score_timesdata,b.total_score as total_score_shanghaidata from wd.timesdata_1 as a  join wd.shanghaidata_1 as b on a.university_name=b.university_name and a.year=b.year;
quit;
run; 

proc sql;
create table wd.total_score_3dataset as 
select a.university_name ,a.year,a.total_score_timesdata,a.total_score_shanghaidata,b.score as total_score_cwurdata  
from wd.total_score_2dataset  as a  join wd.cwurdata_1 as b on a.university_name=b.institution and a.year=b.year;
quit;
run; 
 

*Q1: Sort the "cwurData.csv" dataset on university_name and total_score (descending). Then retain only the highest score for each university in the dataset.;

proc sort data=wd.cwurdata out=wd.cwurdata_descending;
by institution descending score ;
run;

data wd.cwurdata_highest_score;
set wd.cwurdata_descending;
by institution;
if first.institution;
run;
