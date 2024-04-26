create or replace database TRUSTED_ADVISOR_DEV;

create or replace schema DATAMART;

create or replace TABLE COLLEGE_STATEWISE_MARKETING_CAMPAIGN_DATA (
	STATE VARCHAR(100),
	YEAR_OF_CAMPAIGN NUMBER(38,0),
	CAMPAIGN_AMOUNT NUMBER(38,0)
);
create or replace TABLE COMPANY_JOBS_SKILLS (
	"Company" VARCHAR(4194304),
	"Skills" VARCHAR(4194304),
	"Source" VARCHAR(4194304),
	"Experience" VARCHAR(4194304)
);
create or replace TABLE INFRASTRUCTURE_EXPENSE_FACT (
	INFRA_ID NUMBER(4,0),
	TYPE VARCHAR(70),
	DEPARTMENT VARCHAR(60),
	YEAR NUMBER(4,0),
	EXPENSE FLOAT,
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace secure materialized view INFRASTRUCTURE_EXPENSE_VW(
	YEAR,
	TYPE,
	DEPARTMENT,
	EXPENSE_FREQUENCY,
	TOTAL_EXPENSE
) as
select YEAR,TYPE,DEPARTMENT,
       Count(INFRA_ID) AS EXPENSE_FREQUENCY,
       SUM(EXPENSE) AS TOTAL_EXPENSE
       from TRUSTED_ADVISOR_DEV.DATAMART.INFRASTRUCTURE_EXPENSE_FACT
       group by DEPARTMENT,YEAR,TYPE;
create or replace TABLE MENTOR_DIM (
	MENTOR_ID NUMBER(10,0),
	MENTOR_NAME VARCHAR(100),
	GENDER VARCHAR(30),
	AGE_AT_JOINING NUMBER(10,0),
	CONTACT_NO VARCHAR(30),
	POSTAL_CODE NUMBER(10,0),
	QUALIFICATION VARCHAR(50),
	PREVIOUS_YEARS_OF_EXPERIENCE NUMBER(10,0),
	DEPARTMENT VARCHAR(150),
	DESIGNATION VARCHAR(60),
	JOINING_DATE DATE,
	LAST_WORKING_DATE DATE,
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9),
	EMAIL VARCHAR(100)
);
create or replace TABLE MENTOR_DIM_BKP (
	MENTOR_ID NUMBER(10,0),
	MENTOR_NAME VARCHAR(100),
	GENDER VARCHAR(30),
	AGE_AT_JOINING NUMBER(10,0),
	CONTACT_NO VARCHAR(30) MASKING POLICY TRUSTED_ADVISOR_DEV.DATAMART.CONTACT_MASK,
	POSTAL_CODE NUMBER(10,0),
	QUALIFICATION VARCHAR(50),
	PREVIOUS_YEARS_OF_EXPERIENCE NUMBER(10,0),
	DEPARTMENT VARCHAR(150),
	DESIGNATION VARCHAR(60),
	JOINING_DATE DATE,
	LAST_WORKING_DATE DATE,
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9),
	EMAIL VARCHAR(100)
);
create or replace TABLE MENTOR_PAYROLL_FACT (
	MENTOR_ID NUMBER(10,0),
	DESIGNATION VARCHAR(50),
	DEPARTMENT VARCHAR(60),
	YEAR FLOAT,
	MENTOR_SALARY NUMBER(11,0),
	LEAVES_TAKEN NUMBER(6,2),
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace secure materialized view MENTOR_PAYROLL_VW(
	DESIGNATION,
	DEPARTMENT,
	YEAR,
	FACULTY_COUNT,
	TOTAL_LEAVES,
	AGGREGATED_SALARY
) as
select DESIGNATION,
       DEPARTMENT,
       YEAR,
       COUNT(DESIGNATION) AS FACULTY_COUNT,
       COUNT(LEAVES_TAKEN)AS TOTAL_LEAVES,
       SUM(MENTOR_SALARY) AS AGGREGATED_SALARY
       from TRUSTED_ADVISOR_DEV.DATAMART.MENTOR_PAYROLL_FACT
       group by DESIGNATION,DEPARTMENT,YEAR;
create or replace TABLE STUDENT_DIM (
	STUDENT_ROLLNO VARCHAR(60),
	STUDENT_ENROLLMENT_YEAR NUMBER(11,0),
	STUDENT_NAME VARCHAR(100),
	STUDENT_GENDER VARCHAR(30),
	STUDENT_AGE_AT_JOINING NUMBER(11,0),
	STUDENT_STATE VARCHAR(60),
	STUDENT_DISTRICT VARCHAR(60),
	STUDENT_CITY VARCHAR(60),
	STUDENT_POSTAL_CODE NUMBER(11,0),
	STUDENT_EMAIL VARCHAR(60) MASKING POLICY TRUSTED_ADVISOR_DEV.DATAMART.EMAIL_MASK,
	STUDENT_DEGREE VARCHAR(45),
	STUDENT_DEPARTMENT VARCHAR(150),
	STUDENT_X_MARKS FLOAT,
	STUDENT_XII_MARKS FLOAT,
	STUDENT_ENTRANCE_EXAM_MARKS FLOAT,
	STUDENT_FAMILY_SIZE NUMBER(11,0),
	STUDENT_FAMILY_INCOME NUMBER(11,0),
	STUDENT_FATHER_OCCUPATION VARCHAR(60),
	STUDENT_MOTHER_OCCUPATION VARCHAR(60),
	STUDENT_IS_HOSTELLER VARCHAR(15),
	STUDENT_IS_SCHOLARSHIP_AWARDEE VARCHAR(15),
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace materialized view STUDENT_EVENT_ATTENDANCE_VW(
	EVENT_NAME,
	EVENT_TYPE,
	EVENT_HOST,
	EVENT_DATE,
	STUD_ROLL,
	AS_OF_DATE
) as select * from "TRUSTED_ADVISOR_DEV"."EDW"."STUDENT_EVENT_ATTENDANCE";
create or replace TABLE STUDENT_FEEDBACK_FACT (
	ROLLNO VARCHAR(100),
	MENTORID NUMBER(38,0),
	FEEDBACKYEAR NUMBER(4,0),
	FEEDBACKSCORE NUMBER(4,2),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace secure materialized view STUDENT_FEEDBACK_VW(
	MENTORID,
	FEEDBACKYEAR,
	LOWEST_FEEDBACKSCORE,
	HIGHEST_FEEDBACKSCORE,
	NUMBER_OF_FEEDBACKS,
	"AVG_OF_FEEDBACKSCORE$SYS_FACADE$0" COMMENT 'Internal aggregate for AVG_OF_FEEDBACKSCORE',
	"AVG_OF_FEEDBACKSCORE$SYS_FACADE$1" COMMENT 'Internal aggregate for AVG_OF_FEEDBACKSCORE'
) as
select MENTORID,
       FEEDBACKYEAR,
       MIN(FEEDBACKSCORE) AS LOWEST_FEEDBACKSCORE,
       MAX(FEEDBACKSCORE) AS  HIGHEST_FEEDBACKSCORE,
       COUNT(FEEDBACKSCORE) AS NUMBER_OF_FEEDBACKS,
       ROUND(AVG(FEEDBACKSCORE),2) AS AVG_OF_FEEDBACKSCORE
       from TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_FEEDBACK_FACT
       group by  MENTORID,
       FEEDBACKYEAR;
create or replace TABLE STUDENT_INTERNSHIP_FACT (
	ROLLNO VARCHAR(60),
	ENROLLMENT_YEAR NUMBER(11,0),
	DEPARTMENT VARCHAR(150),
	COMPANY1 VARCHAR(100),
	COMPANY2 VARCHAR(100),
	COMPANY3 VARCHAR(100),
	COMPANY4 VARCHAR(100),
	COMPANY5 VARCHAR(100),
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_MENTOR_MAPPING_FACT (
	ROLLNO VARCHAR(60),
	MENTOR_ID NUMBER(11,0),
	YEAR NUMBER(4,0),
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_MENTOR_MAPPING_FACT_BKP (
	ROLLNO VARCHAR(60),
	MENTOR_ID NUMBER(11,0),
	YEAR NUMBER(4,0),
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_PLACEMENT_FACT (
	ROLLNO VARCHAR(60),
	ENROLLMENT_YEAR NUMBER(11,0),
	DEPARTMENT VARCHAR(150),
	IS_PLACED VARCHAR(10),
	IS_PURSUING_HIGHER_EDUCATION VARCHAR(10),
	RECRUITER_COMPANY VARCHAR(100),
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_SCORES_FACT (
	ROLLNO VARCHAR(60),
	ENROLLMENT_YEAR NUMBER(11,0),
	DEPARTMENT VARCHAR(150),
	YEAR NUMBER(4,0),
	SCORE FLOAT,
	GRADE VARCHAR(10),
	ATTENDANCE_PCT FLOAT,
	BACK_SUBJECT VARCHAR(100),
	ENTRY_DATE TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE TWEETS_ANALYSIS_BY_KEYWORD (
	"Keyword" VARCHAR(4194304),
	"Tweet_count" NUMBER(38,0),
	"Polarity_avg" FLOAT,
	"Subjectivity_avg" FLOAT,
	"count" NUMBER(38,0)
);
create or replace view BACK_SUBJECT_STUDENT_AGGREGATE_VW(
	BACK_SUBJECT,
	STUDENT_CNT
) as 
select BACK_SUBJECT , count(BACK_SUBJECT) as STUDENT_CNT 
from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_SCORES_FACT"
where BACK_SUBJECT != 'null'
 group by BACK_SUBJECT having STUDENT_CNT > 100;
create or replace view GRADE_COUNT(
	GRADE,
	ENROLLMENT_YEAR,
	DEPARTMENT,
	YEAR,
	STUDENT_COUNT
) as 
select distinct GRADE,ENROLLMENT_YEAR,DEPARTMENT, YEAR, COUNT(DISTINCT ROLLNO) AS STUDENT_COUNT 
FROM "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_SCORES_FACT" GROUP BY GRADE,ENROLLMENT_YEAR,DEPARTMENT, YEAR;
create or replace view INFRA_EXPENSE_PLACEMENT_CORRELATION_VIEW(
	YEAR_OF_EXPENSE,
	DEPARTMENT,
	TYPE,
	TOTAL_EXPENSE,
	ENROLLMENT_YEAR,
	TOTAL_STUDENTS,
	TOTAL_RECRUITER_COMPANY,
	PLACED_AND_HIGHER_EDU_CNT,
	PLACED_CNT,
	HIGHER_EDU_CNT,
	NONE_CNT,
	PLACEMENT_RATIO
) as
SELECT YEAR_OF_EXPENSE, A.DEPARTMENT, TYPE, TOTAL_EXPENSE, ENROLLMENT_YEAR,TOTAL_STUDENTS, TOTAL_RECRUITER_COMPANY, PLACED_AND_HIGHER_EDU_CNT,  PLACED_CNT, HIGHER_EDU_CNT, NONE_CNT, PLACEMENT_RATIO FROM
(SELECT YEAR AS YEAR_OF_EXPENSE, DEPARTMENT, TYPE, SUM(EXPENSE) AS TOTAL_EXPENSE FROM "TRUSTED_ADVISOR_DEV"."DATAMART"."INFRASTRUCTURE_EXPENSE_FACT" GROUP BY YEAR, DEPARTMENT, TYPE)A
LEFT JOIN
(SELECT  
DEPARTMENT , ENROLLMENT_YEAR ,  
COUNT(DISTINCT ROLLNO) AS TOTAL_STUDENTS,
COUNT(DISTINCT RECRUITER_COMPANY) AS TOTAL_RECRUITER_COMPANY,
SUM(IS_PLACED_AND_HIGHER_EDU_NUM) AS PLACED_AND_HIGHER_EDU_CNT, 
SUM(IS_PLACED_NUM) AS PLACED_CNT,
SUM(IS_HIGHER_EDU_NUM) AS HIGHER_EDU_CNT,
SUM(IS_NOT_PLACED_NUM) AS NONE_CNT,
ROUND((PLACED_AND_HIGHER_EDU_CNT+PLACED_CNT)/TOTAL_STUDENTS,2) AS PLACEMENT_RATIO
FROM 
(select ROLLNO,
    CASE WHEN IS_PLACED ='yes' and  IS_PURSUING_HIGHER_EDUCATION = 'yes' then 1 else 0 end as IS_PLACED_AND_HIGHER_EDU_NUM, 
    CASE WHEN IS_PLACED ='yes' and  IS_PURSUING_HIGHER_EDUCATION = 'no' then 1 else 0 end as IS_PLACED_NUM, 
    CASE WHEN IS_PLACED ='no' and  IS_PURSUING_HIGHER_EDUCATION = 'yes' then 1 else 0 end as IS_HIGHER_EDU_NUM, 
    CASE WHEN IS_PLACED ='no' and  IS_PURSUING_HIGHER_EDUCATION = 'no' then 1 else 0 end as IS_NOT_PLACED_NUM, 
    DEPARTMENT, RECRUITER_COMPANY , ENROLLMENT_YEAR 
 from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_PLACEMENT_FACT")
GROUP BY DEPARTMENT , ENROLLMENT_YEAR )B
ON A.YEAR_OF_EXPENSE=B.ENROLLMENT_YEAR+4
AND A.DEPARTMENT = B.DEPARTMENT;
create or replace view INTERNSHIP_COMPANIES_STUDENT_CNT_VW(
	INTERNSHIP_COMPANY,
	ROLLNO
) as
select distinct COMPANY AS INTERNSHIP_COMPANY , ROLLNO from 
((select * from 
(select distinct COMPANY1 as COMPANY from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT" )t1
left join
(select distinct COMPANY1,ROLLNO  from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT")v1
on t1.COMPANY = v1.COMPANY1)
union
(select * from
(select distinct COMPANY2 as COMPANY from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT" )t2
left join
(select distinct COMPANY2,ROLLNO from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT")v2
on t2.COMPANY= v2.COMPANY2)
union
(select * from
(select distinct COMPANY3 as COMPANY from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT" )t3
left join
(select distinct COMPANY3,ROLLNO from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT")v3
on t3.COMPANY= v3.COMPANY3)
union
(select * from 
(select distinct COMPANY4 as COMPANY from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT" )t4
left join
(select distinct COMPANY4,ROLLNO from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT")v4
on t4.COMPANY = v4.COMPANY4)
union
(select * from 
(select distinct COMPANY5 as COMPANY from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT" )t5
left join
(select distinct COMPANY5,ROLLNO from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT")v5
on t5.COMPANY= v5.COMPANY5))
;
create or replace view MENTOR_STUDENT_PLACEMENT_DETAILS_VW(
	MENTOR_ID,
	TOTAL_STUDENTS,
	PLACED_AND_HIGHER_EDU_CNT,
	PLACED_CNT,
	HIGHER_EDU_CNT,
	NONE_CNT,
	PLACEMENT_RATIO
) as
SELECT MENTOR_ID, 
COUNT (DISTINCT A.ROLLNO) AS TOTAL_STUDENTS, 
SUM(C.IS_PLACED_AND_HIGHER_EDU_NUM) AS PLACED_AND_HIGHER_EDU_CNT, 
SUM(C.IS_PLACED_NUM) AS PLACED_CNT,
SUM(C.IS_HIGHER_EDU_NUM) AS HIGHER_EDU_CNT,
SUM(C.IS_NOT_PLACED_NUM) AS NONE_CNT,
ROUND((PLACED_AND_HIGHER_EDU_CNT+PLACED_CNT)/TOTAL_STUDENTS,2) AS PLACEMENT_RATIO
FROM 
(SELECT DISTINCT MENTOR_ID, ROLLNO FROM "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_MENTOR_MAPPING_FACT")A
JOIN
(select ROLLNO,
    CASE WHEN IS_PLACED ='yes' and  IS_PURSUING_HIGHER_EDUCATION = 'yes' then 1 else 0 end as IS_PLACED_AND_HIGHER_EDU_NUM, 
    CASE WHEN IS_PLACED ='yes' and  IS_PURSUING_HIGHER_EDUCATION = 'no' then 1 else 0 end as IS_PLACED_NUM, 
    CASE WHEN IS_PLACED ='no' and  IS_PURSUING_HIGHER_EDUCATION = 'yes' then 1 else 0 end as IS_HIGHER_EDU_NUM, 
    CASE WHEN IS_PLACED ='no' and  IS_PURSUING_HIGHER_EDUCATION = 'no' then 1 else 0 end as IS_NOT_PLACED_NUM, 
    DEPARTMENT , ENROLLMENT_YEAR 
 from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_PLACEMENT_FACT")c
ON a.ROLLNO=c.ROLLNO
GROUP BY MENTOR_ID;
create or replace secure view MENTOR_VW(
	GENDER,
	QUALIFICATION,
	DEPARTMENT,
	DESIGNATION,
	NUMBER_OF_MENTORS
) as
select  GENDER,
        QUALIFICATION,
        DEPARTMENT,
        DESIGNATION,
        Count(MENTOR_ID) AS NUMBER_OF_MENTORS
 from TRUSTED_ADVISOR_DEV.DATAMART.MENTOR_DIM
 group by GENDER,
        QUALIFICATION,
        PREVIOUS_YEARS_OF_EXPERIENCE,
        DEPARTMENT,
        DESIGNATION;
create or replace view STUDENT_BACK_SUBJECTS_VW(
	ROLLNO,
	YEAR,
	ENROLLMENT_YEAR,
	DEPARTMENT,
	BACK_SUBJECT
) as select ROLLNO, YEAR, ENROLLMENT_YEAR, DEPARTMENT, BACK_SUBJECT from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_SCORES_FACT";
create or replace secure view STUDENT_EDW_SCORES_VW(
	"RollNo",
	"Batch",
	"Scholarship",
	"Department",
	"XMarks",
	"XIIMarks",
	"Year1Score",
	"Year1AttendancePct",
	"Year1Grade",
	"Year1SupplySubjects",
	"Year2Score",
	"Year2AttendancePct",
	"Year2Grade",
	"Year2SupplySubjects",
	"Year3Score",
	"Year3AttendancePct",
	"Year3Grade",
	"Year3SupplySubjects",
	"Year4Score",
	"Year4AttendancePct",
	"Year4Grade",
	"Year4SupplySubjects",
	"CreatedDate",
	AS_OF_DATE
) as
select* from TRUSTED_ADVISOR_DEV.EDW.STUDENT_SCORES;
create or replace view STUDENT_FEEDBACK_AGG_VW(
	ROLLNO,
	"feedbackscore_avg",
	"num_of_feedbacks"
) as
select ROLLNO, avg("FEEDBACKSCORE") as "feedbackscore_avg", count(FEEDBACKSCORE) as "num_of_feedbacks" from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_FEEDBACK_FACT"
group by ROLLNO;
create or replace view STUDENT_INTERNSHIP_COUNT_VW(
	STUDENT_ROLLNO,
	STUDENT_ENROLLMENT_YEAR,
	STUDENT_DEPARTMENT,
	ROLLNO,
	ENROLLMENT_YEAR,
	DEPARTMENT,
	COMPANY1,
	COMPANY2,
	COMPANY3,
	COMPANY4,
	COMPANY5,
	ENTRY_DATE,
	AS_OF_DATE
) as 
select stud_dim.STUDENT_ROLLNO,stud_dim.STUDENT_ENROLLMENT_YEAR,stud_dim.STUDENT_DEPARTMENT, stud_internship.* from 
(SELECT distinct STUDENT_ROLLNO,STUDENT_ENROLLMENT_YEAR,STUDENT_DEPARTMENT from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_DIM")stud_dim
left join
(select * from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_INTERNSHIP_FACT")stud_internship
on stud_dim.STUDENT_ROLLNO=stud_internship.ROLLNO;
create or replace secure view STUDENT_INTERNSHIP_VW(
	ENROLLMENT_YEAR,
	DEPARTMENT,
	COMPANY1,
	TOTAL_STUDENTS_IN_COMPANY1,
	COMPANY2,
	TOTAL_STUDENTS_IN_COMPANY2,
	COMPANY3,
	TOTAL_STUDENTS_IN_COMPANY3,
	COMPANY4,
	TOTAL_STUDENTS_IN_COMPANY4,
	COMPANY5,
	TOTAL_STUDENTS_IN_COMPANY5
) as
select ENROLLMENT_YEAR,
       DEPARTMENT,
       COMPANY1,
       COUNT(COMPANY1) AS TOTAL_STUDENTS_IN_COMPANY1,
       COMPANY2,
       COUNT(COMPANY2) AS TOTAL_STUDENTS_IN_COMPANY2,
       COMPANY3,
       COUNT(COMPANY3) AS TOTAL_STUDENTS_IN_COMPANY3,
       COMPANY4,
       COUNT(COMPANY4) AS TOTAL_STUDENTS_IN_COMPANY4,
       COMPANY5,
       COUNT(COMPANY5) AS TOTAL_STUDENTS_IN_COMPANY5
       from TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_INTERNSHIP_FACT
       group by  ENROLLMENT_YEAR,
       DEPARTMENT,COMPANY1,COMPANY2,COMPANY3,COMPANY4,COMPANY5;
create or replace view STUDENT_MENTOR_MAPPING_INDIVIDUAL_SCORES_VW(
	MENTOR_ID,
	ROLLNO,
	QUALIFICATION,
	DESIGNATION,
	PREVIOUS_YEARS_OF_EXPERIENCE,
	INDIVIDUAL_SCORE
) as
select A.MENTOR_ID, A.ROLLNO, C.QUALIFICATION,C.DESIGNATION, C.PREVIOUS_YEARS_OF_EXPERIENCE,ROUND(B.PER_STUDENT_SCORE,2) AS INDIVIDUAL_SCORE from
(select * from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_MENTOR_MAPPING_FACT")A
join
(select ROLLNO, avg (SCORE) as PER_STUDENT_SCORE from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_SCORES_FACT" group by ROLLNO)B
on A.ROLLNO=B.ROLLNO
JOIN
(SELECT * from "TRUSTED_ADVISOR_DEV"."DATAMART"."MENTOR_DIM")C
ON  A.MENTOR_ID=C.MENTOR_ID;
create or replace view STUDENT_MENTOR_MAPPING_PLACESMENT_STATUS_VW(
	MENTOR_ID,
	ROLLNO,
	IS_PLACED,
	IS_PURSUING_HIGHER_EDUCATION,
	QUALIFICATION,
	DESIGNATION,
	PREVIOUS_YEARS_OF_EXPERIENCE
) as 
select A.MENTOR_ID, B.ROLLNO,B.IS_PLACED,B.IS_PURSUING_HIGHER_EDUCATION  ,C.QUALIFICATION,C.DESIGNATION, C.PREVIOUS_YEARS_OF_EXPERIENCE
from 
"TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_MENTOR_MAPPING_FACT" A
join
"TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_PLACEMENT_FACT" B
on 
A.ROLLNO=B.ROLLNO
join
"TRUSTED_ADVISOR_DEV"."DATAMART"."MENTOR_DIM" C
 ON  A.MENTOR_ID=C.MENTOR_ID;
create or replace secure view STUDENT_MENTOR_MAPPING_VW(
	MENTOR_ID,
	YEAR,
	ENTRY_DATE
) as
select MENTOR_ID,
YEAR,
ENTRY_DATE
from TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_MENTOR_MAPPING_FACT;
create or replace view STUDENT_NOT_ATTENDED_ANY_EVENT_VW(
	STUDENT_ROLLNO,
	ENROLLMENT_YEAR,
	IS_PLACED,
	IS_PURSUING_HIGHER_EDUCATION
) as
select STUDENT_ROLLNO ,ENROLLMENT_YEAR , IS_PLACED, IS_PURSUING_HIGHER_EDUCATION from 
(select distinct STUDENT_ROLLNO from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_DIM" where STUDENT_ENROLLMENT_YEAR in (2016,2017,2018)
minus
select distinct STUD_ROLL from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_EVENT_ATTENDANCE_VW")a
left join
(
    select distinct ROLLNO, ENROLLMENT_YEAR, IS_PLACED, IS_PURSUING_HIGHER_EDUCATION from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_PLACEMENT_FACT"
)b
on a.STUDENT_ROLLNO = b.ROLLNO
;
create or replace secure view STUDENT_PLACEMENT_VW(
	ENROLLMENT_YEAR,
	DEPARTMENT,
	IS_PLACED,
	IS_PURSUING_HIGHER_EDUCATION,
	RECRUITER_COMPANY,
	STUDENTS_PLACED,
	STUDENT_PURSUING_HIGHER_EDUCATION,
	STUDENTS_IN_COMPANIES
) as
select ENROLLMENT_YEAR,
DEPARTMENT,
IS_PLACED,
IS_PURSUING_HIGHER_EDUCATION,
RECRUITER_COMPANY,
COUNT(IS_PLACED) AS STUDENTS_PLACED,
COUNT(IS_PURSUING_HIGHER_EDUCATION) AS STUDENT_PURSUING_HIGHER_EDUCATION,
COUNT(RECRUITER_COMPANY) AS STUDENTS_IN_COMPANIES
from TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_PLACEMENT_FACT
group by ENROLLMENT_YEAR,
DEPARTMENT,
IS_PLACED,
IS_PURSUING_HIGHER_EDUCATION,
RECRUITER_COMPANY;
create or replace view STUDENT_SCORES_VW(
	ROLLNO,
	YEAR,
	ENROLLMENT_YEAR,
	DEPARTMENT,
	SCORE,
	GRADE
) as select Distinct ROLLNO, YEAR, ENROLLMENT_YEAR, DEPARTMENT, SCORE,GRADE from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_SCORES_FACT";
create or replace secure view STUDENT_VW(
	STUDENT_ENROLLMENT_YEAR,
	STUDENT_GENDER,
	STUDENT_STATE,
	STUDENT_DISTRICT,
	STUDENT_DEGREE,
	STUDENT_DEPARTMENT,
	STUDENT_IS_HOSTELLER,
	STUDENT_IS_SCHOLARSHIP_AWARDEE,
	TOTAL_STUDENTS_ENROLLED,
	STUDENT_GENDER_COUNT,
	TOTAL_STUDENTS_IN_DISTRICT,
	TOTAL_STUDENTS_IN_DEGREE,
	TOTAL_STUDENTS_IN_DEPARTMENT,
	LOWEST_X_MARKS,
	HIGHEST_X_MARKS,
	LOWEST_XII_MARKS,
	HIGHEST_XII_MARKS,
	LOWEST_ENTRANCE_EXAM_MARKS,
	HIGHEST_ENTRANCE_EXAM_MARKS,
	TOTAL_STUDENTS_IN_HOSTEL,
	TOTAL_STUDENTS_AWARDED
) as
select STUDENT_ENROLLMENT_YEAR,
       STUDENT_GENDER,
       STUDENT_STATE,
       STUDENT_DISTRICT,
       STUDENT_DEGREE,
       STUDENT_DEPARTMENT,
       STUDENT_IS_HOSTELLER,
       STUDENT_IS_SCHOLARSHIP_AWARDEE,
       COUNT(STUDENT_ENROLLMENT_YEAR) AS TOTAL_STUDENTS_ENROLLED,
       COUNT(STUDENT_GENDER) AS STUDENT_GENDER_COUNT,
       COUNT(STUDENT_DISTRICT) AS TOTAL_STUDENTS_IN_DISTRICT,
       COUNT(STUDENT_DEGREE) AS TOTAL_STUDENTS_IN_DEGREE,
       COUNT(STUDENT_DEPARTMENT) AS TOTAL_STUDENTS_IN_DEPARTMENT,
       MIN(STUDENT_X_MARKS) AS LOWEST_X_MARKS,
       MAX(STUDENT_X_MARKS) AS HIGHEST_X_MARKS,
       MIN(STUDENT_XII_MARKS) AS LOWEST_XII_MARKS,
       MAX(STUDENT_XII_MARKS) AS HIGHEST_XII_MARKS,
       MIN(STUDENT_ENTRANCE_EXAM_MARKS) AS LOWEST_ENTRANCE_EXAM_MARKS,
       MAX(STUDENT_ENTRANCE_EXAM_MARKS) AS HIGHEST_ENTRANCE_EXAM_MARKS,
       COUNT(STUDENT_IS_HOSTELLER) AS TOTAL_STUDENTS_IN_HOSTEL,
       COUNT(STUDENT_IS_SCHOLARSHIP_AWARDEE) AS TOTAL_STUDENTS_AWARDED
       from TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_DIM
       group by STUDENT_ENROLLMENT_YEAR,
       STUDENT_GENDER,
       STUDENT_STATE,
       STUDENT_DISTRICT,
       STUDENT_DEGREE,
       STUDENT_DEPARTMENT,
       STUDENT_ENTRANCE_EXAM_MARKS,
       STUDENT_IS_HOSTELLER,
       STUDENT_IS_SCHOLARSHIP_AWARDEE;
create or replace view STUDENT_WITH_EVENT_ATTENDANCE_COUNT_VW(
	STUDENT_ROLLNO,
	EVENT_COUNT,
	IS_PLACED
) as
select 
STUDENT_ROLLNO,
CASE WHEN STUD_ROLL IS NULL THEN 0 ELSE COUNT END AS EVENT_COUNT,
IS_PLACED
from 
(select STUDENT_ROLLNO from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_DIM" where STUDENT_ENROLLMENT_YEAR between 2016 and 2018)a
join
(select ROLLNO,IS_PLACED  from "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_PLACEMENT_FACT")c
on a.STUDENT_ROLLNO=c.ROLLNO
left join
(select STUD_ROLL,count(1) AS COUNT  from  "TRUSTED_ADVISOR_DEV"."DATAMART"."STUDENT_EVENT_ATTENDANCE_VW" group by STUD_ROLL)b
on a.STUDENT_ROLLNO=b.STUD_ROLL;
CREATE OR REPLACE PROCEDURE "SP_LOAD_MART_INFRASTRUCTURE_EXPENSE_FACT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		var select_src_sql_command = `CREATE OR REPLACE TEMPORARY TABLE TEMP_INFRASTRUCTURE_EXPENSE AS SELECT "Infraid__c","Type__c","Department__c","Year__c","Expense__c","CreatedDate","AS_OF_DATE" FROM(SELECT *, RANK() OVER(PARTITION BY "Infraid__c" ORDER BY "AS_OF_DATE" DESC) RN FROM TRUSTED_ADVISOR_DEV.EDW.INFRASTRUCTURE_EXPENSE)A WHERE A.RN=1;`;
		
        var stmt = snowflake.createStatement( {sqlText: select_src_sql_command} );
        var temp_tbl_resultSet = stmt.execute();
        
        
		//checking for duplicate records
		var dup_chk_sql_command = `SELECT 
		"Infraid__c","Type__c","Department__c","Year__c","Expense__c","CreatedDate","AS_OF_DATE",COUNT(1)
		FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_INFRASTRUCTURE_EXPENSE
        GROUP BY
		"Infraid__c","Type__c","Department__c","Year__c","Expense__c","CreatedDate","AS_OF_DATE"
		HAVING COUNT(1)>1;
			
		 `;
        var stmt = snowflake.createStatement( {sqlText: dup_chk_sql_command} );
        var dup_chk_resultSet = stmt.execute();
		if(dup_chk_resultSet.getRowCount() > 0)
		{
			result =  "Failed: Duplicate records present in source TRUSTED_ADVISOR_DEV.EDW.INFRASTRUCTURE_EXPENSE";
			return result;
		}
		else{
			var tgt_insert_command = ` merge into TRUSTED_ADVISOR_DEV.DATAMART.INFRASTRUCTURE_EXPENSE_FACT using TEMP_INFRASTRUCTURE_EXPENSE 
              on INFRASTRUCTURE_EXPENSE_FACT.INFRA_ID = TEMP_INFRASTRUCTURE_EXPENSE."Infraid__c"
              when matched then 
                  update set 
                  INFRASTRUCTURE_EXPENSE_FACT.TYPE = TEMP_INFRASTRUCTURE_EXPENSE."Type__c",
                  INFRASTRUCTURE_EXPENSE_FACT.DEPARTMENT = TEMP_INFRASTRUCTURE_EXPENSE."Department__c",
                  INFRASTRUCTURE_EXPENSE_FACT.YEAR = TEMP_INFRASTRUCTURE_EXPENSE."Year__c",
                  INFRASTRUCTURE_EXPENSE_FACT.EXPENSE = TEMP_INFRASTRUCTURE_EXPENSE."Expense__c",
                  INFRASTRUCTURE_EXPENSE_FACT.ENTRY_DATE = TEMP_INFRASTRUCTURE_EXPENSE."CreatedDate",
                  INFRASTRUCTURE_EXPENSE_FACT.AS_OF_DATE= TEMP_INFRASTRUCTURE_EXPENSE."AS_OF_DATE"
                  when not matched then insert (INFRA_ID,TYPE,DEPARTMENT,YEAR,EXPENSE,ENTRY_DATE,AS_OF_DATE) 
                                                        VALUES("Infraid__c","Type__c","Department__c","Year__c","Expense__c","CreatedDate","AS_OF_DATE"); `;
			var stmt = snowflake.createStatement( {sqlText: tgt_insert_command} );
			var resultSet = stmt.execute();
			resultSet.next();
			return "Success: SP_LOAD_MART_INFRASTRUCTURE_EXPENSE_FACT executed. "+resultSet.getColumnValue(1)+" rows inserted. "+resultSet.getColumnValue(2)+" rows updated. ";
		}
		
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
CREATE OR REPLACE PROCEDURE "SP_LOAD_MART_MENTOR_DIM"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		var select_src_sql_command = `CREATE OR REPLACE TEMPORARY TABLE TEMP_MENTOR AS SELECT "MentorID","MentorName","Gender","AgeAtJoining","ContactNo","PostalCode","Qualification","YearsOfExperience","Department","Designation",
"JoiningDate","LastWorkingDate","CreatedDate","AS_OF_DATE" FROM(SELECT *, RANK() OVER(PARTITION BY "MentorID" ORDER BY "AS_OF_DATE" DESC) RN FROM TRUSTED_ADVISOR_DEV.EDW.MENTOR)A WHERE A.RN=1;`;
		
        var stmt = snowflake.createStatement( {sqlText: select_src_sql_command} );
        var temp_tbl_resultSet = stmt.execute();
        
        
		//checking for duplicate records
		var dup_chk_sql_command = `SELECT "MentorID","MentorName","Gender","AgeAtJoining","ContactNo","PostalCode","Qualification","YearsOfExperience","Department","Designation","JoiningDate","LastWorkingDate","CreatedDate","AS_OF_DATE",COUNT(1)
			FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_MENTOR
			GROUP BY "MentorID","MentorName","Gender","AgeAtJoining","ContactNo","PostalCode","Qualification","YearsOfExperience","Department","Designation","JoiningDate","LastWorkingDate","CreatedDate","AS_OF_DATE"
			HAVING COUNT(1)>1;
			
		 `;
        var stmt = snowflake.createStatement( {sqlText: dup_chk_sql_command} );
        var dup_chk_resultSet = stmt.execute();
		if(dup_chk_resultSet.getRowCount() > 0)
		{
			result =  "Failed: Duplicate records present in source TRUSTED_ADVISOR_DEV.EDW.MENTOR";
			return result;
		}
		else{
			var tgt_insert_command = `merge into TRUSTED_ADVISOR_DEV.DATAMART.MENTOR_DIM using TEMP_MENTOR 
    on TRUSTED_ADVISOR_DEV.DATAMART.MENTOR_DIM.MENTOR_ID = TEMP_MENTOR."MentorID"
    when matched then 
        update set 
          MENTOR_DIM.MENTOR_NAME = TEMP_MENTOR."MentorName",
          MENTOR_DIM.GENDER=TEMP_MENTOR."Gender",
          MENTOR_DIM.AGE_AT_JOINING=TEMP_MENTOR."AgeAtJoining",
          MENTOR_DIM.CONTACT_NO=TEMP_MENTOR."ContactNo",
          MENTOR_DIM.POSTAL_CODE=TEMP_MENTOR."PostalCode",
          MENTOR_DIM.QUALIFICATION=TEMP_MENTOR."Qualification",
          MENTOR_DIM.PREVIOUS_YEARS_OF_EXPERIENCE=TEMP_MENTOR."YearsOfExperience",
          MENTOR_DIM.DEPARTMENT=TEMP_MENTOR."Department",
          MENTOR_DIM.DESIGNATION=TEMP_MENTOR."Designation",
          MENTOR_DIM.JOINING_DATE=TEMP_MENTOR."JoiningDate",
          MENTOR_DIM.LAST_WORKING_DATE=TEMP_MENTOR."LastWorkingDate",
          MENTOR_DIM.ENTRY_DATE=TEMP_MENTOR."CreatedDate",
          MENTOR_DIM.AS_OF_DATE=TEMP_MENTOR."AS_OF_DATE"
          when not matched then insert (MENTOR_ID,MENTOR_NAME,GENDER,AGE_AT_JOINING,CONTACT_NO,POSTAL_CODE,
                                        QUALIFICATION,PREVIOUS_YEARS_OF_EXPERIENCE,DEPARTMENT,DESIGNATION,
                                        JOINING_DATE,LAST_WORKING_DATE,ENTRY_DATE,AS_OF_DATE) 
                                                VALUES("MentorID","MentorName","Gender","AgeAtJoining","ContactNo","PostalCode","Qualification","YearsOfExperience","Department",                                                  "Designation","JoiningDate","LastWorkingDate","CreatedDate","AS_OF_DATE");`;
			var stmt = snowflake.createStatement( {sqlText: tgt_insert_command} );
			var resultSet = stmt.execute();
			resultSet.next();
			return "Success: SP_LOAD_MART_MENTOR_DIM executed. "+resultSet.getColumnValue(1)+" rows inserted. "+resultSet.getColumnValue(2)+" rows updated. ";
		}
		
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
CREATE OR REPLACE PROCEDURE "SP_LOAD_MART_MENTOR_PAYROLL_FACT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		var select_src_sql_command = `CREATE OR REPLACE TEMPORARY TABLE TEMP_MENTOR_PAYROLL AS SELECT "MentorID__c","Designation__c","Department__c","Year__c","Salary__c","LeavesTaken__c","CreatedDate","AS_OF_DATE" FROM(SELECT *, RANK() OVER(PARTITION BY "MentorID__c" ORDER BY "AS_OF_DATE" DESC) RN FROM TRUSTED_ADVISOR_DEV.EDW.MENTOR_PAYROLL)A WHERE A.RN=1;`;
		
        var stmt = snowflake.createStatement( {sqlText: select_src_sql_command} );
        var temp_tbl_resultSet = stmt.execute();
        
        
		//checking for duplicate records
		var dup_chk_sql_command = `SELECT 
		"MentorID__c","Designation__c","Department__c","Year__c","Salary__c","LeavesTaken__c","CreatedDate","AS_OF_DATE",COUNT(1)
		FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_MENTOR_PAYROLL
        GROUP BY
		"MentorID__c","Designation__c","Department__c","Year__c","Salary__c","LeavesTaken__c","CreatedDate","AS_OF_DATE"
		HAVING COUNT(1)>1;			
		 `;
        var stmt = snowflake.createStatement( {sqlText: dup_chk_sql_command} );
        var dup_chk_resultSet = stmt.execute();
		if(dup_chk_resultSet.getRowCount() > 0)
		{
			result =  "Failed: Duplicate records present in source TRUSTED_ADVISOR_DEV.EDW.MENTOR_PAYROLL";
			return result;
		}
		else{
			var tgt_insert_command = `merge into TRUSTED_ADVISOR_DEV.DATAMART.MENTOR_PAYROLL_FACT using TEMP_MENTOR_PAYROLL 
					on MENTOR_PAYROLL_FACT.MENTOR_ID = TEMP_MENTOR_PAYROLL."MentorID__c"
					when matched then 
						update set 
						MENTOR_PAYROLL_FACT.DESIGNATION = TEMP_MENTOR_PAYROLL."Designation__c",
                        MENTOR_PAYROLL_FACT.DEPARTMENT = TEMP_MENTOR_PAYROLL."Department__c",
                        MENTOR_PAYROLL_FACT.YEAR = TEMP_MENTOR_PAYROLL."Year__c",
                        MENTOR_PAYROLL_FACT.MENTOR_SALARY = TEMP_MENTOR_PAYROLL."Salary__c",
                        MENTOR_PAYROLL_FACT.LEAVES_TAKEN = TEMP_MENTOR_PAYROLL."LeavesTaken__c",
                        MENTOR_PAYROLL_FACT.ENTRY_DATE = TEMP_MENTOR_PAYROLL."CreatedDate",
						MENTOR_PAYROLL_FACT.AS_OF_DATE= TEMP_MENTOR_PAYROLL."AS_OF_DATE"
						when not matched then insert (MENTOR_ID,DESIGNATION,DEPARTMENT,YEAR,MENTOR_SALARY,LEAVES_TAKEN,ENTRY_DATE,AS_OF_DATE) 
									VALUES("MentorID__c","Designation__c","Department__c","Year__c","Salary__c","LeavesTaken__c","CreatedDate","AS_OF_DATE");  `;
			var stmt = snowflake.createStatement( {sqlText: tgt_insert_command} );
			var resultSet = stmt.execute();
			resultSet.next();
			return "Success: SP_LOAD_MART_MENTOR_PAYROLL_FACT executed. "+resultSet.getColumnValue(1)+" rows inserted. "+resultSet.getColumnValue(2)+" rows updated. ";
		}
		
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
CREATE OR REPLACE PROCEDURE "SP_LOAD_MART_STUDENT_DIM"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		var select_src_sql_command = `CREATE OR REPLACE TEMPORARY TABLE TEMP_STUDENT AS SELECT "RollNo","EnrollmentYear","StudentName","Gender","AgeAtEnrollment","State", "District", "City", "PostalCode","Email","Degree","Department","XMarks","XIIMarks",
"EntranceExamMarks","FamilySize","FamilyIncome","FatherOccupation","MotherOccupation","IsHosteller","IsScholarshipAwardee","CreatedDate","AS_OF_DATE"
FROM(SELECT *, RANK() OVER(PARTITION BY "RollNo" ORDER BY "AS_OF_DATE" DESC) RN FROM TRUSTED_ADVISOR_DEV.EDW.STUDENT)A WHERE A.RN=1;`;
		
        var stmt = snowflake.createStatement( {sqlText: select_src_sql_command} );
        var temp_tbl_resultSet = stmt.execute();
        
        
		//checking for duplicate records
		var dup_chk_sql_command = `SELECT 
		"RollNo",
		"EnrollmentYear",
		"StudentName",
		"Gender",
		"AgeAtEnrollment",
		"State", 
		"District", 
		"City", 
		"PostalCode",
		"Email",
		"Degree",
		"Department",
		"XMarks",
		"XIIMarks",
		"EntranceExamMarks",
		"FamilySize",
		"FamilyIncome",
		"FatherOccupation",
		"MotherOccupation",
		"IsHosteller",
		"IsScholarshipAwardee",
		"CreatedDate",
		"AS_OF_DATE",
		COUNT(1)
		FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_STUDENT
		GROUP BY 
		"RollNo",
		"EnrollmentYear",
		"StudentName",
		"Gender",
		"AgeAtEnrollment",
		"State", 
		"District", 
		"City", 
		"PostalCode",
		"Email",
		"Degree",
		"Department",
		"XMarks",
		"XIIMarks",
		"EntranceExamMarks",
		"FamilySize",
		"FamilyIncome",
		"FatherOccupation",
		"MotherOccupation",
		"IsHosteller",
		"IsScholarshipAwardee",
		"CreatedDate",
		"AS_OF_DATE"
		HAVING COUNT(1)>1;
			
		 `;
        var stmt = snowflake.createStatement( {sqlText: dup_chk_sql_command} );
        var dup_chk_resultSet = stmt.execute();
		if(dup_chk_resultSet.getRowCount() > 0)
		{
			result =  "Failed: Duplicate records present in source TRUSTED_ADVISOR_DEV.EDW.STUDENT";
			return result;
		}
		else{
			var tgt_insert_command = `merge into TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_DIM using TEMP_STUDENT 
    on TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_DIM.STUDENT_ROLLNO = TEMP_STUDENT."RollNo"
    when matched then 
        update set 
        STUDENT_DIM.STUDENT_ENROLLMENT_YEAR = TEMP_STUDENT."EnrollmentYear",
        STUDENT_DIM.STUDENT_NAME = TEMP_STUDENT."StudentName",
        STUDENT_DIM.STUDENT_GENDER = TEMP_STUDENT."Gender",
        STUDENT_DIM.STUDENT_AGE_AT_JOINING = TEMP_STUDENT."AgeAtEnrollment",
        STUDENT_DIM.STUDENT_STATE = TEMP_STUDENT."State",
		STUDENT_DIM.STUDENT_DISTRICT = TEMP_STUDENT."District",
		STUDENT_DIM.STUDENT_CITY = TEMP_STUDENT."City",
		STUDENT_DIM.STUDENT_POSTAL_CODE = TEMP_STUDENT."PostalCode",
		STUDENT_DIM.STUDENT_EMAIL = TEMP_STUDENT."Email",
		STUDENT_DIM.STUDENT_DEGREE = TEMP_STUDENT."Degree",
		STUDENT_DIM.STUDENT_DEPARTMENT = TEMP_STUDENT."Department",
		STUDENT_DIM.STUDENT_X_MARKS = TEMP_STUDENT."XMarks",
		STUDENT_DIM.STUDENT_XII_MARKS = TEMP_STUDENT."XIIMarks",
		STUDENT_DIM.STUDENT_ENTRANCE_EXAM_MARKS = TEMP_STUDENT."EntranceExamMarks",
		STUDENT_DIM.STUDENT_FAMILY_SIZE = TEMP_STUDENT."FamilySize",
		STUDENT_DIM.STUDENT_FAMILY_INCOME = TEMP_STUDENT."FamilyIncome",
		STUDENT_DIM.STUDENT_FATHER_OCCUPATION = TEMP_STUDENT."FatherOccupation",
	    STUDENT_DIM.STUDENT_MOTHER_OCCUPATION = TEMP_STUDENT."MotherOccupation",
		STUDENT_DIM.STUDENT_IS_HOSTELLER = TEMP_STUDENT."IsHosteller",
		STUDENT_DIM.STUDENT_IS_SCHOLARSHIP_AWARDEE = TEMP_STUDENT."IsScholarshipAwardee",
		STUDENT_DIM.ENTRY_DATE = TEMP_STUDENT."CreatedDate",
		STUDENT_DIM.AS_OF_DATE = TEMP_STUDENT."AS_OF_DATE"
		when not matched then insert (STUDENT_ROLLNO,STUDENT_ENROLLMENT_YEAR,STUDENT_NAME,STUDENT_GENDER,STUDENT_AGE_AT_JOINING,
                                      STUDENT_STATE,STUDENT_DISTRICT,STUDENT_CITY,STUDENT_POSTAL_CODE,STUDENT_EMAIL,STUDENT_DEGREE,
                                      STUDENT_DEPARTMENT,STUDENT_X_MARKS,STUDENT_XII_MARKS,STUDENT_ENTRANCE_EXAM_MARKS,STUDENT_FAMILY_SIZE,
                                      STUDENT_FAMILY_INCOME,STUDENT_FATHER_OCCUPATION,STUDENT_MOTHER_OCCUPATION,STUDENT_IS_HOSTELLER,
                                      STUDENT_IS_SCHOLARSHIP_AWARDEE,ENTRY_DATE,AS_OF_DATE) 
                                      VALUES("RollNo","EnrollmentYear","StudentName","Gender","AgeAtEnrollment","State", "District", "City", 
                                             "PostalCode","Email","Degree","Department","XMarks","XIIMarks","EntranceExamMarks","FamilySize",
                                             "FamilyIncome","FatherOccupation","MotherOccupation","IsHosteller","IsScholarshipAwardee","CreatedDate","AS_OF_DATE");`;
			var stmt = snowflake.createStatement( {sqlText: tgt_insert_command} );
			var resultSet = stmt.execute();
			resultSet.next();
			return "Success: SP_LOAD_MART_STUDENT_DIM executed. "+resultSet.getColumnValue(1)+" rows inserted. "+resultSet.getColumnValue(2)+" rows updated. ";
		}
		
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
CREATE OR REPLACE PROCEDURE "SP_LOAD_MART_STUDENT_FEEDBACK_FACT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		var select_src_sql_command = `CREATE OR REPLACE TEMPORARY TABLE TEMP_STUDENT_FEEDBACK AS SELECT "ROLLNO","MENTORID","FEEDBACKYEAR","FEEDBACKSCORE","AS_OF_DATE" FROM(SELECT *, RANK() OVER(PARTITION BY "ROLLNO","MENTORID","FEEDBACKYEAR" ORDER BY "AS_OF_DATE" DESC) RN FROM TRUSTED_ADVISOR_DEV.EDW.STUDENT_FEEDBACK)A WHERE A.RN=1;`;
		
        var stmt = snowflake.createStatement( {sqlText: select_src_sql_command} );
        var temp_tbl_resultSet = stmt.execute();
        
        
		//checking for duplicate records
		var dup_chk_sql_command = `SELECT 
		"ROLLNO","MENTORID","FEEDBACKYEAR","FEEDBACKSCORE","AS_OF_DATE",COUNT(1)
		FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_STUDENT_FEEDBACK
        GROUP BY
		"ROLLNO","MENTORID","FEEDBACKYEAR","FEEDBACKSCORE","AS_OF_DATE"
		HAVING COUNT(1)>1;			
		 `;
        var stmt = snowflake.createStatement( {sqlText: dup_chk_sql_command} );
        var dup_chk_resultSet = stmt.execute();
		if(dup_chk_resultSet.getRowCount() > 0)
		{
			result =  "Failed: Duplicate records present in source TRUSTED_ADVISOR_DEV.EDW.STUDENT_FEEDBACK";
			return result;
		}
		else{
			var tgt_insert_command = `merge into TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_FEEDBACK_FACT using TEMP_STUDENT_FEEDBACK 
					on STUDENT_FEEDBACK_FACT.ROLLNO = TEMP_STUDENT_FEEDBACK."ROLLNO"
					and STUDENT_FEEDBACK_FACT.MENTORID = TEMP_STUDENT_FEEDBACK."MENTORID"
					and STUDENT_FEEDBACK_FACT.FEEDBACKYEAR = TEMP_STUDENT_FEEDBACK."FEEDBACKYEAR"
					when matched then 
						update set 
					 
						STUDENT_FEEDBACK_FACT.FEEDBACKSCORE = TEMP_STUDENT_FEEDBACK."FEEDBACKSCORE",
						STUDENT_FEEDBACK_FACT.AS_OF_DATE= TEMP_STUDENT_FEEDBACK."AS_OF_DATE"
						when not matched then insert ("ROLLNO","MENTORID","FEEDBACKYEAR","FEEDBACKSCORE","AS_OF_DATE") 
															  VALUES("ROLLNO","MENTORID","FEEDBACKYEAR","FEEDBACKSCORE","AS_OF_DATE");  `;
			var stmt = snowflake.createStatement( {sqlText: tgt_insert_command} );
			var resultSet = stmt.execute();
			resultSet.next();
			return "Success: SP_LOAD_MART_STUDENT_FEEDBACK_FACT executed. "+resultSet.getColumnValue(1)+" rows inserted. "+resultSet.getColumnValue(2)+" rows updated. ";
		}
		
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
CREATE OR REPLACE PROCEDURE "SP_LOAD_MART_STUDENT_INTERNSHIP_FACT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		var select_src_sql_command = `CREATE OR REPLACE TEMPORARY TABLE TEMP_STUDENT_INTERNSHIP AS SELECT "RollNo","Batch","Department","Company1","Company2","Company3", "Company4", "Company5","CreatedDate","AS_OF_DATE" FROM(SELECT *, RANK() OVER(PARTITION BY "RollNo" ORDER BY "AS_OF_DATE" DESC) RN FROM TRUSTED_ADVISOR_DEV.EDW.STUDENT_INTERNSHIP)A WHERE A.RN=1;`;
		
        var stmt = snowflake.createStatement( {sqlText: select_src_sql_command} );
        var temp_tbl_resultSet = stmt.execute();
        
        
		//checking for duplicate records
		var dup_chk_sql_command = `SELECT 
		"RollNo",
		"Batch",
		"Department",
		"Company1",
		"Company2",
		"Company3", 
		"Company4", 
		"Company5",
		"CreatedDate",
		"AS_OF_DATE",
		COUNT(1)
		FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_STUDENT_INTERNSHIP
        GROUP BY
		"RollNo",
		"Batch",
		"Department",
		"Company1",
		"Company2",
		"Company3", 
		"Company4", 
		"Company5",
		"CreatedDate",
		"AS_OF_DATE"
		HAVING COUNT(1)>1;
			
		 `;
        var stmt = snowflake.createStatement( {sqlText: dup_chk_sql_command} );
        var dup_chk_resultSet = stmt.execute();
		if(dup_chk_resultSet.getRowCount() > 0)
		{
			result =  "Failed: Duplicate records present in source TRUSTED_ADVISOR_DEV.EDW.STUDENT_INTERNSHIP";
			return result;
		}
		else{
			var tgt_insert_command = `merge into TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_INTERNSHIP_FACT using TEMP_STUDENT_INTERNSHIP 
                on STUDENT_INTERNSHIP_FACT.ROLLNO = TEMP_STUDENT_INTERNSHIP."RollNo"
                when matched then 
                    update set 
            STUDENT_INTERNSHIP_FACT.ENROLLMENT_YEAR = TEMP_STUDENT_INTERNSHIP."Batch",
            STUDENT_INTERNSHIP_FACT.DEPARTMENT = TEMP_STUDENT_INTERNSHIP."Department",
            STUDENT_INTERNSHIP_FACT.COMPANY1 = TEMP_STUDENT_INTERNSHIP."Company1",
            STUDENT_INTERNSHIP_FACT.COMPANY2 = TEMP_STUDENT_INTERNSHIP."Company2",
            STUDENT_INTERNSHIP_FACT.COMPANY3 = TEMP_STUDENT_INTERNSHIP."Company3",
            STUDENT_INTERNSHIP_FACT.COMPANY4 = TEMP_STUDENT_INTERNSHIP."Company4",
            STUDENT_INTERNSHIP_FACT.COMPANY5 = TEMP_STUDENT_INTERNSHIP."Company5",
            STUDENT_INTERNSHIP_FACT.ENTRY_DATE = TEMP_STUDENT_INTERNSHIP."CreatedDate",
            STUDENT_INTERNSHIP_FACT.AS_OF_DATE= TEMP_STUDENT_INTERNSHIP."AS_OF_DATE"
            when not matched then insert (ROLLNO,ENROLLMENT_YEAR,DEPARTMENT,COMPANY1,COMPANY2,COMPANY3,COMPANY4,COMPANY5,ENTRY_DATE,AS_OF_DATE) 
                                                  VALUES("RollNo","Batch","Department","Company1","Company2","Company3", "Company4", "Company5","CreatedDate","AS_OF_DATE")`;
			var stmt = snowflake.createStatement( {sqlText: tgt_insert_command} );
			var resultSet = stmt.execute();
			resultSet.next();
			return "Success: SP_LOAD_MART_STUDENT_INTERNSHIP_FACT executed. "+resultSet.getColumnValue(1)+" rows inserted. "+resultSet.getColumnValue(2)+" rows updated.";
		}
		
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
CREATE OR REPLACE PROCEDURE "SP_LOAD_MART_STUDENT_MENTOR_MAPPING_FACT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		var select_src_sql_command = `CREATE OR REPLACE TEMPORARY TABLE TEMP_MENTOR_STUDENT_MAPPING AS SELECT "RollNo","MentorID","MentorResigns","CreatedDate","AS_OF_DATE" FROM(SELECT *, RANK() OVER(PARTITION BY "RollNo","MentorID" ORDER BY "AS_OF_DATE" DESC) RN FROM TRUSTED_ADVISOR_DEV.EDW.STUDENT_MENTOR_MAPPING)A WHERE A.RN=1;`;
		
        var stmt = snowflake.createStatement( {sqlText: select_src_sql_command} );
        var temp_tbl_resultSet = stmt.execute();
        
        
		//checking for duplicate records
		var dup_chk_sql_command = `SELECT 
		"RollNo","MentorID","MentorResigns","CreatedDate","AS_OF_DATE",COUNT(1)
		FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_MENTOR_STUDENT_MAPPING
        GROUP BY
		"RollNo","MentorID","MentorResigns","CreatedDate","AS_OF_DATE"
		HAVING COUNT(1)>1;			
		 `;
        var stmt = snowflake.createStatement( {sqlText: dup_chk_sql_command} );
        var dup_chk_resultSet = stmt.execute();
		if(dup_chk_resultSet.getRowCount() > 0)
		{
			result =  "Failed: Duplicate records present in source TRUSTED_ADVISOR_DEV.EDW.STUDENT_MENTOR_MAPPING";
			return result;
		}
		else{
			var tgt_insert_command = `merge into TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_MENTOR_MAPPING_FACT using TEMP_MENTOR_STUDENT_MAPPING 
					on STUDENT_MENTOR_MAPPING_FACT.ROLLNO = TEMP_MENTOR_STUDENT_MAPPING."RollNo"
					and STUDENT_MENTOR_MAPPING_FACT.MENTOR_ID = TEMP_MENTOR_STUDENT_MAPPING."MentorID"
					when matched then 
						update set 
						STUDENT_MENTOR_MAPPING_FACT.YEAR = TEMP_MENTOR_STUDENT_MAPPING."MentorResigns",
                        STUDENT_MENTOR_MAPPING_FACT.ENTRY_DATE = TEMP_MENTOR_STUDENT_MAPPING."CreatedDate",
						STUDENT_MENTOR_MAPPING_FACT.AS_OF_DATE= TEMP_MENTOR_STUDENT_MAPPING."AS_OF_DATE"
						when not matched then insert (ROLLNO,MENTOR_ID,YEAR,ENTRY_DATE,AS_OF_DATE) 
									VALUES("RollNo","MentorID","MentorResigns","CreatedDate","AS_OF_DATE");  `;
			var stmt = snowflake.createStatement( {sqlText: tgt_insert_command} );
			var resultSet = stmt.execute();
			resultSet.next();
			return "Success: SP_LOAD_MART_STUDENT_MENTOR_MAPPING_FACT executed. "+resultSet.getColumnValue(1)+" rows inserted. "+resultSet.getColumnValue(2)+" rows updated. ";
		}
		
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
CREATE OR REPLACE PROCEDURE "SP_LOAD_MART_STUDENT_PLACEMENT_FACT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		var select_src_sql_command = `CREATE OR REPLACE TEMPORARY TABLE TEMP_STUDENT_PLACEMENT AS SELECT "RollNo","Batch","Department","IsPlaced","IsPursuingHigherEducation","Company","CreatedDate","AS_OF_DATE" FROM(SELECT *, RANK() OVER(PARTITION BY "RollNo" ORDER BY "AS_OF_DATE" DESC) RN FROM TRUSTED_ADVISOR_DEV.EDW.STUDENT_PLACEMENT)A WHERE A.RN=1;`;
		
        var stmt = snowflake.createStatement( {sqlText: select_src_sql_command} );
        var temp_tbl_resultSet = stmt.execute();
        
        
		//checking for duplicate records
		var dup_chk_sql_command = `SELECT 
		"RollNo","Batch","Department","IsPlaced","IsPursuingHigherEducation","Company","CreatedDate","AS_OF_DATE",COUNT(1)
		FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_STUDENT_PLACEMENT
        GROUP BY
		"RollNo","Batch","Department","IsPlaced","IsPursuingHigherEducation","Company","CreatedDate","AS_OF_DATE"
		HAVING COUNT(1)>1;
			
		 `;
        var stmt = snowflake.createStatement( {sqlText: dup_chk_sql_command} );
        var dup_chk_resultSet = stmt.execute();
		if(dup_chk_resultSet.getRowCount() > 0)
		{
			result =  "Failed: Duplicate records present in source TRUSTED_ADVISOR_DEV.EDW.STUDENT_PLACEMENT";
			return result;
		}
		else{
			var tgt_insert_command = `merge into TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_PLACEMENT_FACT using TEMP_STUDENT_PLACEMENT 
              on STUDENT_PLACEMENT_FACT.ROLLNO = TEMP_STUDENT_PLACEMENT."RollNo"
              when matched then 
                  update set 
                  STUDENT_PLACEMENT_FACT.ENROLLMENT_YEAR = TEMP_STUDENT_PLACEMENT."Batch",
                  STUDENT_PLACEMENT_FACT.DEPARTMENT = TEMP_STUDENT_PLACEMENT."Department",
                  STUDENT_PLACEMENT_FACT.IS_PLACED = TEMP_STUDENT_PLACEMENT."IsPlaced",
                  STUDENT_PLACEMENT_FACT.IS_PURSUING_HIGHER_EDUCATION = TEMP_STUDENT_PLACEMENT."IsPursuingHigherEducation",
                  STUDENT_PLACEMENT_FACT.RECRUITER_COMPANY = TEMP_STUDENT_PLACEMENT."Company",
                  STUDENT_PLACEMENT_FACT.ENTRY_DATE = TEMP_STUDENT_PLACEMENT."CreatedDate",
                  STUDENT_PLACEMENT_FACT.AS_OF_DATE= TEMP_STUDENT_PLACEMENT."AS_OF_DATE"
                  when not matched then insert (ROLLNO,ENROLLMENT_YEAR,DEPARTMENT,IS_PLACED,IS_PURSUING_HIGHER_EDUCATION,RECRUITER_COMPANY,ENTRY_DATE,AS_OF_DATE) 
                                                        VALUES("RollNo","Batch","Department","IsPlaced","IsPursuingHigherEducation","Company","CreatedDate","AS_OF_DATE");`;
			var stmt = snowflake.createStatement( {sqlText: tgt_insert_command} );
			var resultSet = stmt.execute();
			resultSet.next();
			return "Success: SP_LOAD_MART_STUDENT_PLACEMENT_FACT executed. "+resultSet.getColumnValue(1)+" rows inserted. "+resultSet.getColumnValue(2)+" rows updated. ";
		}
		
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
CREATE OR REPLACE PROCEDURE "SP_LOAD_MART_STUDENT_SCORES_FACT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		var select_src_sql_command = `SELECT "RollNo", "Batch","Department","Year1Score","Year1AttendancePct","Year1Grade","Year1SupplySubjects","Year2Score","Year2AttendancePct","Year2Grade","Year2SupplySubjects","Year3Score","Year3AttendancePct","Year3Grade","Year3SupplySubjects","Year4Score","Year4AttendancePct","Year4Grade","Year4SupplySubjects",CAST("CreatedDate" AS VARCHAR(100)),CAST("AS_OF_DATE" AS VARCHAR(100)) FROM(SELECT *, RANK() OVER(PARTITION BY "RollNo" ORDER BY "AS_OF_DATE" DESC) RN FROM TRUSTED_ADVISOR_DEV.EDW.STUDENT_SCORES)A WHERE A.RN=1;`;
		var stmt = snowflake.createStatement( {sqlText: select_src_sql_command} );
		var src_resultSet = stmt.execute();
		var resultArray = []
		while (src_resultSet.next())  {
			var rollNo = src_resultSet.getColumnValue(1);
			var batch = src_resultSet.getColumnValue(2);
			var dept = src_resultSet.getColumnValue(3);
			var entryDate = src_resultSet.getColumnValue(20);
			var asOfDate = src_resultSet.getColumnValue(21);
            k=1
			for (let i = 1; i < 16; i+=4) {
				var year = k++
				var score = src_resultSet.getColumnValue(3+i);
                if(score==null || score == undefined || score.toString().trim().length == 0)
                {
                    continue;
                }
				var attendance = src_resultSet.getColumnValue(3+i+1);
				var grade = src_resultSet.getColumnValue(3+i+2);
                var backSubjectsArray = [];
                if (src_resultSet.getColumnValue(3+i+3) != undefined)
                {
                    backSubjectsArray = src_resultSet.getColumnValue(3+i+3).split('','');
                }
				
                if(backSubjectsArray.length == 0)
                {
                    var rec = 
					{
						ROLLNO: rollNo,
						ENROLLMENT_YEAR: batch,
						DEPARTMENT: dept,
						YEAR: year,
						SCORE: score,
						GRADE: grade,
						ATTENDANCE_PCT: attendance,
						BACK_SUBJECT: null,
						ENTRY_DATE: entryDate,
						AS_OF_DATE: asOfDate
					}
					resultArray.push(rec);
                }
				for (let j= 0; j < backSubjectsArray.length; j+=1)
				{
					var rec = 
					{
						ROLLNO: rollNo,
						ENROLLMENT_YEAR: batch,
						DEPARTMENT: dept,
						YEAR: year,
						SCORE: score,
						GRADE: grade,
						ATTENDANCE_PCT: attendance,
						BACK_SUBJECT: backSubjectsArray[j].trim(),
						ENTRY_DATE: entryDate,
						AS_OF_DATE: asOfDate
					}
					resultArray.push(rec);
				}	
			}	
		}
        var temp_tbl_sql_command = `CREATE OR REPLACE TEMPORARY TABLE TEMP_STUD_SCORES AS SELECT * FROM DATAMART.STUDENT_SCORES_FACT WHERE 1=2`;
        var stmt = snowflake.createStatement( {sqlText: temp_tbl_sql_command} );
        var temp_tbl_resultSet = stmt.execute();
        var temp_insert_command = `INSERT INTO TEMP_STUD_SCORES(ROLLNO,ENROLLMENT_YEAR,DEPARTMENT,YEAR,SCORE,GRADE,ATTENDANCE_PCT,BACK_SUBJECT,ENTRY_DATE,AS_OF_DATE) values `;
        for (let i = 0; i < resultArray.length-1; i++)
        {
                temp_insert_command += ("(''"+resultArray[i].ROLLNO+"'',"+
                    resultArray[i].ENROLLMENT_YEAR+","+
                "''"+resultArray[i].DEPARTMENT+"'',"+
                    resultArray[i].YEAR+","+
                    resultArray[i].SCORE+","+
                "''"+resultArray[i].GRADE+"'',"+
                    resultArray[i].ATTENDANCE_PCT+","+
                "''"+resultArray[i].BACK_SUBJECT+"'',"+
                "''"+resultArray[i].ENTRY_DATE+"'',"+
                "''"+resultArray[i].AS_OF_DATE+"''),")
        }
        temp_insert_command += ("(''"+resultArray[resultArray.length-1].ROLLNO+"'',"+
                resultArray[resultArray.length-1].ENROLLMENT_YEAR+","+
            "''"+resultArray[resultArray.length-1].DEPARTMENT+"'',"+
                resultArray[resultArray.length-1].YEAR+","+
                resultArray[resultArray.length-1].SCORE+","+
            "''"+resultArray[resultArray.length-1].GRADE+"'',"+
                resultArray[resultArray.length-1].ATTENDANCE_PCT+","+
            "''"+resultArray[resultArray.length-1].BACK_SUBJECT+"'',"+
            "''"+resultArray[resultArray.length-1].ENTRY_DATE+"'',"+
            "''"+resultArray[resultArray.length-1].AS_OF_DATE+"'');")
       
        var stmt = snowflake.createStatement( {sqlText: temp_insert_command} );
        var temp_resultSet = stmt.execute();
        
		//checking for duplicate records
		var dup_chk_sql_command = `
			SELECT 
			ROLLNO,
			ENROLLMENT_YEAR,
			DEPARTMENT,
			YEAR,
			SCORE,
			GRADE,
			ATTENDANCE_PCT,
			BACK_SUBJECT,
			ENTRY_DATE,
			AS_OF_DATE,
			COUNT(1)
			FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_STUD_SCORES
			GROUP BY
			ROLLNO,
			ENROLLMENT_YEAR,
			DEPARTMENT,
			YEAR,
			SCORE,
			GRADE,
			ATTENDANCE_PCT,
			BACK_SUBJECT,
			ENTRY_DATE,
			AS_OF_DATE
			HAVING COUNT(1)>1;
			
		 `;
        var stmt = snowflake.createStatement( {sqlText: dup_chk_sql_command} );
        var dup_chk_resultSet = stmt.execute();
		if(dup_chk_resultSet.getRowCount() > 0)
		{
			result =  "Failed: Duplicate records present in source TRUSTED_ADVISOR_DEV.EDW.STUDENT_SCORES.";
			return result;
		}
		else{
			var truncate_tgt_command = `TRUNCATE TABLE TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_SCORES_FACT;`;
			var stmt = snowflake.createStatement( {sqlText: truncate_tgt_command} );
			var resultSet = stmt.execute();
			var tgt_insert_command = `INSERT INTO TRUSTED_ADVISOR_DEV.DATAMART.STUDENT_SCORES_FACT(ROLLNO,ENROLLMENT_YEAR,DEPARTMENT,YEAR,SCORE,GRADE,ATTENDANCE_PCT,BACK_SUBJECT,ENTRY_DATE,AS_OF_DATE) SELECT ROLLNO,ENROLLMENT_YEAR,DEPARTMENT,YEAR,SCORE,GRADE,ATTENDANCE_PCT,BACK_SUBJECT,ENTRY_DATE,AS_OF_DATE FROM TRUSTED_ADVISOR_DEV.DATAMART.TEMP_STUD_SCORES`;
			var stmt = snowflake.createStatement( {sqlText: tgt_insert_command} );
			var resultSet = stmt.execute();
			resultSet.next();
			return "Success: SP_LOAD_MART_STUDENT_SCORES_FACT executed. "+resultSet.getColumnValue(1)+" rows loaded.";
		}
		
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
create or replace masking policy CONTACT_MASK as (CONTACTNO VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE WHEN
    CURRENT_ROLE() in ('ANALYST_ROLE','QA_ROLE','ACCOUNTADMIN','PC_MATILLION_ROLE','DEV_SYSADMIN') THEN CONTACTNO
    ELSE '********'
END
;
create or replace masking policy EMAIL_MASK as (EMAIL VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE WHEN
    CURRENT_ROLE() in ('ANALYST_ROLE','QA_ROLE','ACCOUNTADMIN','PC_MATILLION_ROLE','DEV_SYSADMIN') THEN EMAIL
    ELSE '********'
END
;
create or replace masking policy INCOME_MASK as (INCOME NUMBER(38,0)) 
returns NUMBER(38,0) ->
CASE WHEN
    CURRENT_ROLE() in ('ANALYST_ROLE','QA_ROLE','ACCOUNTADMIN','PC_MATILLION_ROLE','DEV_SYSADMIN') THEN INCOME
    ELSE 0000
END
;
create or replace schema DATASCI_TEMP;

create or replace TABLE TWEETS_LANG_FILTERED_TWITTER_SENTIMENT (
	"Original" VARCHAR(4194304),
	"EN_Translated" VARCHAR(4194304)
);
create or replace TABLE TWEETS_PREPARED_TWITTER_SENTIMENT (
	"Original" VARCHAR(4194304),
	"EN_Translated" VARCHAR(4194304)
);
create or replace TABLE TWEETS_SENTIMENT_ANALYSIS_TWITTER_SENTIMENT (
	"Tweet" VARCHAR(4194304),
	"Keyword" VARCHAR(4194304),
	"Polarity" FLOAT,
	"Subjectivity" FLOAT
);
create or replace schema EDW;

create or replace TABLE INFRASTRUCTURE_EXPENSE (
	"Infraid__c" FLOAT,
	"Type__c" VARCHAR(70),
	"Department__c" VARCHAR(60),
	"Year__c" FLOAT,
	"Expense__c" FLOAT,
	"CreatedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE MENTOR (
	"MentorID" NUMBER(10,0),
	"MentorName" VARCHAR(90),
	"Qualification" VARCHAR(60),
	"YearsOfExperience" NUMBER(10,0),
	"Designation" VARCHAR(60),
	"ContactNo" VARCHAR(30),
	"AgeAtJoining" NUMBER(10,0),
	"Gender" VARCHAR(30),
	"PostalCode" NUMBER(10,0),
	"Department" VARCHAR(150),
	"JoiningDate" DATE,
	"LastWorkingDate" DATE,
	"CreatedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE MENTOR_BKP (
	"MentorID" NUMBER(10,0),
	"MentorName" VARCHAR(90),
	"Qualification" VARCHAR(60),
	"YearsOfExperience" NUMBER(10,0),
	"Designation" VARCHAR(60),
	"ContactNo" VARCHAR(30) MASKING POLICY TRUSTED_ADVISOR_DEV.EDW.CONTACT_MASK,
	"AgeAtJoining" NUMBER(10,0),
	"Gender" VARCHAR(30),
	"PostalCode" NUMBER(10,0),
	"Department" VARCHAR(150),
	"JoiningDate" DATE,
	"LastWorkingDate" DATE,
	"CreatedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE MENTOR_PAYROLL (
	"MentorID__c" FLOAT,
	"Designation__c" VARCHAR(50),
	"Salary__c" NUMBER(18,2),
	"ContactNo__c" VARCHAR(18) MASKING POLICY TRUSTED_ADVISOR_DEV.EDW.CONTACT_MASK,
	"Year__c" FLOAT,
	"Gender__c" VARCHAR(10),
	"Department__c" VARCHAR(60),
	"LeavesTaken__c" FLOAT,
	"JoiningDate__c" DATE,
	"LastWorkingDate__c" DATE,
	"CreatedDate" TIMESTAMP_NTZ(9),
	"LastModifiedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE MENTOR_PAYROLL_BKP (
	"MentorID__c" FLOAT,
	"Designation__c" VARCHAR(50),
	"Salary__c" NUMBER(18,2),
	"ContactNo__c" VARCHAR(18),
	"Year__c" FLOAT,
	"Gender__c" VARCHAR(10),
	"Department__c" VARCHAR(60),
	"LeavesTaken__c" FLOAT,
	"JoiningDate__c" DATE,
	"LastWorkingDate__c" DATE,
	"CreatedDate" TIMESTAMP_NTZ(9),
	"LastModifiedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace secure materialized view STG_TWITTER_FEED_MV(
	CREATED_AT,
	ID,
	TEXT
) as SELECT 
    
    twt.value:created_at::STRING as created_at,
    twt.value: id::NUMBER as id,
    twt.value:text::STRING as text
FROM  landing.STG_TWITTER_FEED, lateral flatten(input => FEED_JSON:data) twt;
create or replace TABLE STUDENT (
	"RollNo" VARCHAR(60),
	"StudentName" VARCHAR(90),
	"Gender" VARCHAR(30),
	"AgeAtEnrollment" NUMBER(11,0),
	"State" VARCHAR(60),
	"District" VARCHAR(60),
	"City" VARCHAR(60),
	"PostalCode" NUMBER(11,0),
	"FamilyIncome" NUMBER(11,0),
	"PhoneNumber" VARCHAR(60) MASKING POLICY TRUSTED_ADVISOR_DEV.EDW.CONTACT_MASK,
	"Email" VARCHAR(60) MASKING POLICY TRUSTED_ADVISOR_DEV.EDW.EMAIL_MASK,
	"EnrollmentYear" NUMBER(11,0),
	"Degree" VARCHAR(45),
	"Department" VARCHAR(150),
	"XMarks" FLOAT,
	"XIIMarks" FLOAT,
	"EntranceExamMarks" FLOAT,
	"FamilySize" NUMBER(11,0),
	"FatherOccupation" VARCHAR(60),
	"MotherOccupation" VARCHAR(60),
	"IsHosteller" VARCHAR(15),
	"IsScholarshipAwardee" VARCHAR(15),
	"CreatedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_EVENT_ATTENDANCE (
	EVENT_NAME VARCHAR(100),
	EVENT_TYPE VARCHAR(100),
	EVENT_HOST VARCHAR(100),
	EVENT_DATE DATE,
	STUD_ROLL VARCHAR(10),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_FEEDBACK (
	ROLLNO VARCHAR(100),
	MENTORID NUMBER(38,0),
	FEEDBACKYEAR NUMBER(4,0),
	FEEDBACKSCORE NUMBER(4,2),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_INTERNSHIP (
	"RollNo" VARCHAR(60),
	"Batch" NUMBER(11,0),
	"Department" VARCHAR(150),
	"Company1" VARCHAR(300),
	"Company2" VARCHAR(300),
	"Company3" VARCHAR(300),
	"Company4" VARCHAR(300),
	"Company5" VARCHAR(300),
	"CreatedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_MENTOR_MAPPING (
	"RollNo" VARCHAR(60),
	"MentorID" NUMBER(11,0),
	"MentorResigns" NUMBER(11,0),
	"CreatedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_PLACEMENT (
	"RollNo" VARCHAR(60),
	"Batch" NUMBER(11,0),
	"Department" VARCHAR(150),
	"IsPlaced" VARCHAR(15),
	"IsPursuingHigherEducation" VARCHAR(15),
	"Company" VARCHAR(300),
	"CreatedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE STUDENT_SCORES (
	"RollNo" VARCHAR(60),
	"Batch" NUMBER(11,0),
	"Scholarship" VARCHAR(15),
	"Department" VARCHAR(150),
	"XMarks" FLOAT,
	"XIIMarks" FLOAT,
	"Year1Score" FLOAT,
	"Year1AttendancePct" NUMBER(11,0),
	"Year1Grade" VARCHAR(3),
	"Year1SupplySubjects" VARCHAR(3000),
	"Year2Score" FLOAT,
	"Year2AttendancePct" NUMBER(11,0),
	"Year2Grade" VARCHAR(3),
	"Year2SupplySubjects" VARCHAR(3000),
	"Year3Score" FLOAT,
	"Year3AttendancePct" NUMBER(11,0),
	"Year3Grade" VARCHAR(3),
	"Year3SupplySubjects" VARCHAR(3000),
	"Year4Score" FLOAT,
	"Year4AttendancePct" NUMBER(11,0),
	"Year4Grade" VARCHAR(3),
	"Year4SupplySubjects" VARCHAR(3000),
	"CreatedDate" TIMESTAMP_NTZ(9),
	AS_OF_DATE TIMESTAMP_NTZ(9)
);
create or replace view STUDENT_EVENT_ATTENDANCE_INVALID(
	EVENT_NAME,
	"dest_EVENT_NAME",
	"dest_EVENT_HOST",
	"dest_EVENT_DATE",
	"dest_STUD_ROLL",
	"dest_EVENT_TYPE",
	"dest_AS_OF_DATE",
	"Indicator"
) as (SELECT 
  "EVENT_NAME", 
  "dest_EVENT_NAME", 
  "dest_EVENT_HOST", 
  "dest_EVENT_DATE", 
  "dest_STUD_ROLL", 
  "dest_EVENT_TYPE", 
  "dest_AS_OF_DATE", 
  "Indicator" 
FROM (SELECT 	"EVENT_NAME",
	"dest_EVENT_NAME",
	"dest_EVENT_HOST",
	cast ("dest_EVENT_DATE" as DATE) as "dest_EVENT_DATE",
	"dest_STUD_ROLL",
	"dest_EVENT_TYPE",
	"dest_AS_OF_DATE",
	"Indicator"
FROM
	(SELECT 
	  * 
	FROM ((SELECT 
	  * 
	FROM ((SELECT
	 COALESCE ("A"."EVENT_NAME", "B"."EVENT_NAME") AS "EVENT_NAME",
	 "B"."EVENT_NAME"
	 AS "dest_EVENT_NAME", "B"."EVENT_HOST"
	 AS "dest_EVENT_HOST", "B"."EVENT_DATE"
	 AS "dest_EVENT_DATE", "B"."STUD_ROLL"
	 AS "dest_STUD_ROLL", "B"."EVENT_TYPE"
	 AS "dest_EVENT_TYPE", "A"."AS_OF_DATE"
	 AS "dest_AS_OF_DATE", CASE WHEN "A"."EVENT_NAME" IS NULL
	 THEN CAST('N' AS VARCHAR(1)) WHEN "B"."EVENT_NAME" IS NULL
	 THEN CAST('D' AS VARCHAR(1)) WHEN ("A"."EVENT_NAME" = "B"."EVENT_NAME" OR ("A"."EVENT_NAME" IS NULL AND "B"."EVENT_NAME" IS NULL)) AND ("A"."EVENT_HOST" = "B"."EVENT_HOST" OR ("A"."EVENT_HOST" IS NULL AND "B"."EVENT_HOST" IS NULL)) AND ("A"."EVENT_DATE" = "B"."EVENT_DATE" OR ("A"."EVENT_DATE" IS NULL AND "B"."EVENT_DATE" IS NULL)) AND ("A"."STUD_ROLL" = "B"."STUD_ROLL" OR ("A"."STUD_ROLL" IS NULL AND "B"."STUD_ROLL" IS NULL)) AND ("A"."EVENT_TYPE" = "B"."EVENT_TYPE" OR ("A"."EVENT_TYPE" IS NULL AND "B"."EVENT_TYPE" IS NULL))
	 THEN CAST('I' AS VARCHAR(1)) ELSE CAST('C' AS VARCHAR(1)) END AS "Indicator"
	FROM
	 (SELECT 
	  "EVENT_NAME", 
	  "EVENT_TYPE", 
	  "EVENT_HOST", 
	  "EVENT_DATE", 
	  "STUD_ROLL", 
	  "AS_OF_DATE" 
	FROM "TRUSTED_ADVISOR_DEV"."EDW"."STUDENT_EVENT_ATTENDANCE") AS "A"
	 FULL JOIN
	 (SELECT 
	  "EVENT_NAME", 
	  "EVENT_HOST", 
	  "EVENT_DATE", 
	  "STUD_ROLL", 
	  "EVENT_TYPE" 
	FROM "TRUSTED_ADVISOR_DEV"."LANDING"."STG_STUDENT_EVENT_ATTENDANCE") AS "B"
	 ON
	 "A"."EVENT_NAME" = "B"."EVENT_NAME"
	 WHERE
	 ("A"."EVENT_NAME" IS NOT NULL)
	 OR
	 ("B"."EVENT_NAME" IS NOT NULL))) 
	WHERE ("Indicator" = 'N' 
	      OR "Indicator" = 'C'))) 
	WHERE (("dest_EVENT_NAME" IS NULL 
	      OR "dest_EVENT_NAME" = '') 
	      OR ("dest_EVENT_HOST" IS NULL 
	      OR "dest_EVENT_HOST" = '') 
	      OR ("dest_EVENT_DATE" IS NULL 
	      OR "dest_EVENT_DATE" = '') 
	      OR ("dest_STUD_ROLL" IS NULL 
	      OR "dest_STUD_ROLL" = '') 
	      OR ("dest_EVENT_TYPE" IS NULL 
	      OR "dest_EVENT_TYPE" = '')))) AS "v_0000000178_0000000640");
CREATE OR REPLACE FILE FORMAT TSV_SKIPHEADER_FF
	FIELD_DELIMITER = '\t'
	SKIP_HEADER = 1
	TRIM_SPACE = TRUE
;
create or replace masking policy CONTACT_MASK as (CONTACTNO VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE WHEN
    CURRENT_ROLE() in ('ANALYST_ROLE','QA_ROLE','ACCOUNTADMIN','PC_MATILLION_ROLE','DEV_SYSADMIN') THEN CONTACTNO
    ELSE '********'
END
;
create or replace masking policy EMAIL_MASK as (EMAIL VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE WHEN
    CURRENT_ROLE() in ('ANALYST_ROLE','QA_ROLE','ACCOUNTADMIN','PC_MATILLION_ROLE','DEV_SYSADMIN') THEN EMAIL
    ELSE '********'
END
;
create or replace masking policy INCOME_MASK as (INCOME NUMBER(38,0)) 
returns NUMBER(38,0) ->
CASE WHEN
    CURRENT_ROLE() in ('ANALYST_ROLE','QA_ROLE','ACCOUNTADMIN','PC_MATILLION_ROLE','DEV_SYSADMIN') THEN INCOME
    ELSE 0000
END
;
create or replace schema LANDING;

create or replace TRANSIENT TABLE API_LOGS (
	ID NUMBER(38,0),
	TYPE VARCHAR(100),
	JOB_NAME VARCHAR(200),
	COMPONENT_ID NUMBER(38,0),
	COMPONENT_NAME VARCHAR(200),
	STATE VARCHAR(20),
	START_TIME TIMESTAMP_NTZ(9),
	END_TIME TIMESTAMP_NTZ(9),
	MESSAGE VARCHAR(200)
);
create or replace TABLE AUTOMATED_TESTING_MAPPING (
	SOURCE_SCHEMA VARCHAR(50),
	SOURCE_TABLE VARCHAR(100),
	TARGET_SCHEMA VARCHAR(50),
	TARGET_TABLE VARCHAR(100),
	INVALID_RECORD_VIEW VARCHAR(100),
	TEST_QUERY VARCHAR(16777216),
	JOB_CODE NUMBER(4,0)
);
create or replace TABLE AUTOMATED_TESTING_RESULT (
	SOURCE_SCHEMA VARCHAR(50),
	SOURCE_TABLE VARCHAR(100),
	TARGET_SCHEMA VARCHAR(50),
	TARGET_TABLE VARCHAR(100),
	NO_OF_ROWS_NOT_LOADED NUMBER(38,0),
	INVALID_RECORD_COUNT NUMBER(38,0),
	LOAD_DATE TIMESTAMP_NTZ(9)
);
create or replace TRANSIENT TABLE CUSTOM_LOGS (
	ID VARCHAR(2000),
	TYPE VARCHAR(2000),
	JOB_NAME VARCHAR(2000),
	COMPONENT_ID VARCHAR(2000),
	COMPONENT_NAME VARCHAR(2000),
	STATE VARCHAR(2000),
	START_TIME VARCHAR(2000),
	END_TIME VARCHAR(2000),
	MESSAGE VARCHAR(2000),
	NOTIFICATION_SENT VARCHAR(2000)
);
create or replace TABLE MAPPING (
	RDS_SOURCE VARCHAR(50),
	LANDING_TARGET VARCHAR(50)
);
create or replace TRANSIENT TABLE STG_INFRASTRUCTURE_EXPENSE (
	"Infraid__c" FLOAT,
	"Type__c" VARCHAR(70),
	"Department__c" VARCHAR(60),
	"Year__c" FLOAT,
	"Expense__c" FLOAT,
	"CreatedDate" TIMESTAMP_NTZ(9)
);
create or replace TABLE STG_MENTOR (
	"MentorID" NUMBER(10,0),
	"MentorName" VARCHAR(90),
	"Qualification" VARCHAR(60),
	"YearsOfExperience" NUMBER(10,0),
	"Designation" VARCHAR(60),
	"ContactNo" VARCHAR(30),
	"AgeAtJoining" NUMBER(10,0),
	"Gender" VARCHAR(30),
	"PostalCode" NUMBER(10,0),
	"Department" VARCHAR(150),
	"JoiningDate" DATE,
	"LastWorkingDate" DATE,
	"CreatedDate" TIMESTAMP_NTZ(9)
);
create or replace TRANSIENT TABLE STG_MENTOR_BKP (
	"MentorID" NUMBER(10,0),
	"MentorName" VARCHAR(90),
	"Qualification" VARCHAR(60),
	"YearsOfExperience" NUMBER(10,0),
	"Designation" VARCHAR(60),
	"ContactNo" VARCHAR(30) MASKING POLICY TRUSTED_ADVISOR_DEV.LANDING.CONTACT_MASK,
	"AgeAtJoining" NUMBER(10,0),
	"Gender" VARCHAR(30),
	"PostalCode" NUMBER(10,0),
	"Department" VARCHAR(150),
	"JoiningDate" DATE,
	"LastWorkingDate" DATE,
	"CreatedDate" TIMESTAMP_NTZ(9)
);
create or replace TRANSIENT TABLE STG_MENTOR_PAYROLL (
	"MentorID__c" FLOAT,
	"Designation__c" VARCHAR(50),
	"Salary__c" NUMBER(18,2),
	"ContactNo__c" VARCHAR(18) MASKING POLICY TRUSTED_ADVISOR_DEV.LANDING.CONTACT_MASK,
	"Year__c" FLOAT,
	"Gender__c" VARCHAR(10),
	"Department__c" VARCHAR(60),
	"LeavesTaken__c" FLOAT,
	"JoiningDate__c" DATE,
	"LastWorkingDate__c" DATE,
	"CreatedDate" TIMESTAMP_NTZ(9),
	"LastModifiedDate" TIMESTAMP_NTZ(9)
);
create or replace TRANSIENT TABLE STG_STUDENT (
	"RollNo" VARCHAR(60),
	"StudentName" VARCHAR(90),
	"Gender" VARCHAR(30),
	"AgeAtEnrollment" NUMBER(11,0),
	"State" VARCHAR(60),
	"District" VARCHAR(60),
	"City" VARCHAR(60),
	"PostalCode" NUMBER(11,0),
	"FamilyIncome" NUMBER(11,0),
	"PhoneNumber" VARCHAR(60) MASKING POLICY TRUSTED_ADVISOR_DEV.LANDING.CONTACT_MASK,
	"Email" VARCHAR(60) MASKING POLICY TRUSTED_ADVISOR_DEV.LANDING.EMAIL_MASK,
	"EnrollmentYear" NUMBER(11,0),
	"Degree" VARCHAR(45),
	"Department" VARCHAR(150),
	"XMarks" FLOAT,
	"XIIMarks" FLOAT,
	"EntranceExamMarks" FLOAT,
	"FamilySize" NUMBER(11,0),
	"FatherOccupation" VARCHAR(60),
	"MotherOccupation" VARCHAR(60),
	"IsHosteller" VARCHAR(15),
	"IsScholarshipAwardee" VARCHAR(15),
	"CreatedDate" TIMESTAMP_NTZ(9)
);
create or replace TRANSIENT TABLE STG_STUDENT_EVENT_ATTENDANCE (
	EVENT_NAME VARCHAR(100),
	EVENT_HOST VARCHAR(100),
	EVENT_DATE VARCHAR(50),
	STUD_ROLL VARCHAR(10),
	EVENT_TYPE VARCHAR(50),
	EVENT_DATE_MOD VARCHAR(20)
);
create or replace TRANSIENT TABLE STG_STUDENT_FEEDBACK (
	ROLLNO VARCHAR(20),
	MENTORID NUMBER(38,0),
	FEEDBACKYEAR NUMBER(4,0),
	FEEDBACKSCORE NUMBER(4,2)
);
create or replace TRANSIENT TABLE STG_STUDENT_INTERNSHIP (
	"RollNo" VARCHAR(60),
	"Batch" NUMBER(11,0),
	"Department" VARCHAR(150),
	"Company1" VARCHAR(300),
	"Company2" VARCHAR(300),
	"Company3" VARCHAR(300),
	"Company4" VARCHAR(300),
	"Company5" VARCHAR(300),
	"CreatedDate" TIMESTAMP_NTZ(9)
);
create or replace TRANSIENT TABLE STG_STUDENT_MENTOR_MAPPING (
	"RollNo" VARCHAR(60),
	"MentorID" NUMBER(11,0),
	"MentorResigns" NUMBER(11,0),
	"CreatedDate" TIMESTAMP_NTZ(9)
);
create or replace TRANSIENT TABLE STG_STUDENT_PLACEMENT (
	"RollNo" VARCHAR(60),
	"Batch" NUMBER(11,0),
	"Department" VARCHAR(150),
	"IsPlaced" VARCHAR(15),
	"IsPursuingHigherEducation" VARCHAR(15),
	"Company" VARCHAR(300),
	"CreatedDate" TIMESTAMP_NTZ(9)
);
create or replace TRANSIENT TABLE STG_STUDENT_SCORES (
	"RollNo" VARCHAR(60),
	"Batch" NUMBER(11,0),
	"Scholarship" VARCHAR(15),
	"Department" VARCHAR(150),
	"XMarks" FLOAT,
	"XIIMarks" FLOAT,
	"Year1Score" FLOAT,
	"Year1AttendancePct" NUMBER(11,0),
	"Year1Grade" VARCHAR(3),
	"Year1SupplySubjects" VARCHAR(3000),
	"Year2Score" FLOAT,
	"Year2AttendancePct" NUMBER(11,0),
	"Year2Grade" VARCHAR(3),
	"Year2SupplySubjects" VARCHAR(3000),
	"Year3Score" FLOAT,
	"Year3AttendancePct" NUMBER(11,0),
	"Year3Grade" VARCHAR(3),
	"Year3SupplySubjects" VARCHAR(3000),
	"Year4Score" FLOAT,
	"Year4AttendancePct" NUMBER(11,0),
	"Year4Grade" VARCHAR(3),
	"Year4SupplySubjects" VARCHAR(3000),
	"CreatedDate" TIMESTAMP_NTZ(9)
);
create or replace TABLE STG_STUD_FEEDBACK_SEED (
	QUESTION VARCHAR(1000),
	FEEDBACK VARCHAR(100),
	PHONE_NUMBER VARCHAR(50),
	TIMESTAMP VARCHAR(50)
);
create or replace TRANSIENT TABLE STG_TWITTER_FEED (
	FEED_JSON VARIANT
);
create or replace TRANSIENT TABLE STG_TWITTER_FEED_BCKUP (
	FEED_JSON VARIANT
);
create or replace secure materialized view STG_TWITTER_FEED_MV(
	CREATED_AT,
	ID,
	TEXT
) as SELECT 
    
    twt.value:created_at::STRING as created_at,
    twt.value: id::NUMBER as id,
    twt.value:text::STRING as text
FROM  landing.STG_TWITTER_FEED, lateral flatten(input => FEED_JSON:data) twt;
create or replace TABLE TEMP (
	TIMESTP DATE
);
CREATE OR REPLACE FILE FORMAT CSV_SKIPHEADER_FF
	SKIP_HEADER = 1
	TRIM_SPACE = TRUE
;
CREATE OR REPLACE FILE FORMAT STUD_EVENT_CSV
	SKIP_HEADER = 1
	TRIM_SPACE = TRUE
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
;
CREATE OR REPLACE FILE FORMAT TSV_FF
	FIELD_DELIMITER = '\t'
	SKIP_HEADER = 1
	TRIM_SPACE = TRUE
;
CREATE OR REPLACE FILE FORMAT TWITTER_DATA_FF
	TYPE = JSON
	NULL_IF = ()
	STRIP_OUTER_ARRAY = TRUE
;
CREATE OR REPLACE EXTERNAL FUNCTION "HELLOEMAILWORLD"("SENDER" VARCHAR(16777216), "RECEIVER" VARCHAR(16777216), "SUBJECT" VARCHAR(16777216), "BODY_TEXT" VARCHAR(16777216))
RETURNS VARIANT
API_INTEGRATION = "EMAIL_API"
AS 'https://4n6va7exz1.execute-api.ap-southeast-1.amazonaws.com/dev/helloworld';
CREATE OR REPLACE PROCEDURE "SP_SEND_ACCOUNT_STATS_EMAIL"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	try
	{
		emailText = ''''
				
		var src_login_hist_command = `SELECT DISTINCT USER_NAME,IS_SUCCESS,ERROR_CODE,ERROR_MESSAGE FROM "SNOWFLAKE"."ACCOUNT_USAGE"."LOGIN_HISTORY" WHERE DATE(EVENT_TIMESTAMP) >= CURRENT_DATE and EVENT_TYPE = ''LOGIN'';`;
		
        var stmt = snowflake.createStatement( {sqlText: src_login_hist_command} );
        var resultSet = stmt.execute();
        
		if(resultSet.getRowCount() > 0)
		{
			i = 1
			emailText += ''Following users logged in to Snowflake Account today:-<br><br>''
			while(resultSet.next())
			{
				USER_NAME = resultSet.getColumnValue(1)
				IS_SUCCESS = resultSet.getColumnValue(2)
				ERROR_CODE = resultSet.getColumnValue(3)
				ERROR_MESSAGE = resultSet.getColumnValue(4)
				
				
				if(IS_SUCCESS == ''YES'')
				{
					emailText+=((i++) + ''.''+USER_NAME+'' logged in successfully.<br>-----------------------------------------------------------<br>'')
				}
				else
				{
					emailText+=((i++) + ''.''+USER_NAME+'' attempted but failed to log in. Error Code: ''+ERROR_CODE+''. Error Message: ''+ERROR_MESSAGE+''.<br>-----------------------------------------------------------<br>'')
				}
				
			}
		}
		if(emailText.length > 0)
		{
			subject = ''Daily Updates from Snowflake Account'' ;
			var send_email_command = "SELECT TRUSTED_ADVISOR_DEV.LANDING.HELLOEMAILWORLD(''adwitiya.a.das@kipi.bi'',''adwitiya.a.das@kipi.bi'',''"+subject+"'',''"+emailText+"'');"
			var stmt = snowflake.createStatement( {sqlText: send_email_command} );
			var result = stmt.execute();
		}
		else
		{
			subject = ''Daily Updates from Snowflake Account'' ;
			emailText = ''No queries run with System-defined roles. No one logged in to snowflake account'';
			var send_email_command = "SELECT TRUSTED_ADVISOR_DEV.LANDING.HELLOEMAILWORLD(''adwitiya.a.das@kipi.bi'',''adwitiya.a.das@kipi.bi'',''"+subject+"'',''"+emailText+"'');"
			var stmt = snowflake.createStatement( {sqlText: send_email_command} );
			var result = stmt.execute();
		}
		return ''SP_SEND_ACCOUNT_STATS_EMAIL() executed successfully. Email sent to admin''
	}
	catch (err)  
	{
	  result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
	  result += "\\\\n  Message: " + err.message;
	  result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
	  return result;
	}
';
create or replace pipe STUD_EVENT_PIPE auto_ingest=true as copy into TRUSTED_ADVISOR_DEV.LANDING.STG_STUDENT_EVENT_ATTENDANCE
  from @TRUSTED_ADVISOR_DEV.LANDING.STUD_EVENT_STAGE
  file_format = TRUSTED_ADVISOR_DEV.LANDING.STUD_EVENT_CSV;
create or replace pipe TWITTER_DATA_SNOWPIPE auto_ingest=true error_integration='ERROR_INT' as copy into STG_TWITTER_FEED
   from @Twitter_data_stage;
create or replace task SEND_LOGIN_HISTORY_TASK
	schedule='USING CRON 45 06 * * * UTC'
	as call SP_SEND_ACCOUNT_STATS_EMAIL();
create or replace task T1
	schedule='USING CRON 15 06 * * * UTC'
	as call SP_SEND_ACCOUNT_STATS_EMAIL();
create or replace task T2
	schedule='USING CRON 40 15 * * * UTC'
	as insert into temp values(current_date);
create or replace masking policy CONTACT_MASK as (CONTACTNO VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE WHEN
    CURRENT_ROLE() in ('ANALYST_ROLE','QA_ROLE','ACCOUNTADMIN','PC_MATILLION_ROLE','DEV_SYSADMIN') THEN CONTACTNO
    ELSE '********'
END
;
create or replace masking policy EMAIL_MASK as (EMAIL VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE WHEN
    CURRENT_ROLE() in ('ANALYST_ROLE','QA_ROLE','ACCOUNTADMIN','PC_MATILLION_ROLE','DEV_SYSADMIN') THEN EMAIL
    ELSE '********'
END
;
create or replace masking policy INCOME_MASK as (INCOME NUMBER(38,0)) 
returns NUMBER(38,0) ->
CASE WHEN
    CURRENT_ROLE() in ('ANALYST_ROLE','QA_ROLE','ACCOUNTADMIN','PC_MATILLION_ROLE','DEV_SYSADMIN') THEN INCOME
    ELSE 0000
END
;
create or replace schema PUBLIC;
