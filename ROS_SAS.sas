## 파일 업로드 
PROC IMPORT datafile='/folders/myfolders/ROS/BFC_100.csv'
dbms=csv 
out=BFC;
GETNAMES = Yes;
RUN;

############################################# STEP1: 데이터 전처리   
1) ROS, ATO가 있는 T30, T60 데이터 추출 (KEY, DRUG, TOT) 
2) LIST; T20과 각각 조인 후 합치기 (+ 요양개시일자) 
3)ROSATO: 병용하는 경우 ID LIST
4) GROUP: 단일복용하는 경우만 추출: ROS- ROSATO/ ATO- ROSATO (ID, KEY, 요양개시일자, DRUG, TOT)
5) DATE FORMAT CHANGE 
###########################################################################################################
#1 ROS, ATO가 있는 T30, T60 데이터 추출 (KEY, DRUG, TOT) 
#1-1) T30추출
%macro yearmonth30;
data m1;
set 
%do yr= 2005 %to 2019; 
%do i= 0 %to 0;
%do qr=1 %to 9;
	REQ000046708_T30_&yr&i&qr
%end;
%end;
%end;
;
run;
data m2;
set 
%do yr= 2005 %to 2019; 
%do qr=10 %to 12;
	REQ000046708_T30_&yr&qr
%end;
%end;
;
run;
data T30;
set m1 m2;
run;

PROC SQL;
drop table M1;
drop table M2;
QUIT;
%mend;
%yearmonth30;

PROC SQL;
CREATE TABLE RA30 AS 
SELECET CMN_KEY AS KEY, MCARE_DIV_CD_ADJ AS DRUG, TOT_MCNT AS TOT 
FROM T30 
WHERE  MCARE_DIV_CD_ADJ =4540 OR MCARE_DIV_CD_ADJ =1115;
QUIT;
PROC SQL;
DROP TABLE T30;
QUIT;

#1-2) COMBIND T60 

%macro yearmonth60;
data m1;
set 
%do yr= 2005 %to 2019; 
%do i= 0 %to 0;
%do qr=1 %to 9;
	REQ000046708_T60_&yr&i&qr
%end;
%end;
%end;
;
run;
data m2;
set 
%do yr= 2005 %to 2019; 
%do qr=10 %to 12;
	REQ000046708_T60_&yr&qr
%end;
%end;
;
run;
data T60;
set m1 m2;
run;

PROC SQL;
drop table M1;
drop table M2;
QUIT;
%mend;
%yearmonth60;

PROC SQL;
CREATE TABLE RA60 AS 
SELECT CMN_KEY AS KEY, MCARE_DIV_CD_ADJ AS DRUG
FROM T60 
WHERE  MCARE_DIV_CD_ADJ =4540 OR MCARE_DIV_CD_ADJ =1115;
QUIT;
PROC SQL ;
DROP TABLE T60;
QUIT;

#1-3) COMBINE T30 & T60 
PROC SQL;
CREATE TABLE RA3060 AS 
SELECT DISTINCT * FROM RA30
UNION ALL 
SELECT DISTINCT * FROM RA60;
QUIT;
PROC SQL;
DROP TABLE RA30;
DROP TABLE RA60;
QUIT;

#2 LIST; T20과 각각 조인 후 합치기 (+ 요양개시일자)
#3 ROSATO: 병용하는 경우 ID LIST
#4 GROUP: 단일복용하는 경우만 추출: ROS- ROSATO/ ATO- ROSATO (ID, KEY, 요양개시일자, DRUG, TOT)
PROC SQL; 
CREATE TABLE LIST AS 
SELECT distinct B.INDI_DSCM_NO AS ID, A.KEY, B.MDCARE_ST_RT_DT AS DATE, A.DRUG, B.TOT_PRES_DD_CNT AS T20_TOT
FROM RA3060  A  INNER JOIN T20 B
ON ( A.KEY = B.CMN_KEY ) 
ORDER BY B.INDI_DSCM_NO ASC; 

CREATE TABLE CNT AS 
SELECT DISTINCT ID, COUNT(DISTINCT DRUG) AS CNT 
FROM LIST GROUP BY ID; 

CREATE TABLE GROUP AS 
SELECT DISTINCT A.* 
FROM LIST A  
INNER JOIN  CNT B
ON A.ID = B.ID
WHERE B.CNT=1; 

ALTER TABLE GROUP ADD ROS INT;
UPDATE GROUP SET ROS =1 WHERE DRUG =4540;

QUIT; 

PROC SQL;
DROP TABLE CNT;
DROP TABLE LIST;
QUIT;

#5 CHANGE DATE FORMAT
PROC SQL;
ALTER TABLE
SET GROUP;
date = input(put(DATE,8.), yymmdd8.);
datesix= date+180;
format date yymmdd8.;
format datesix yymmdd8.;
run;

PROC SQL;
DROP TABLE RA3060;
ALTER TABLE LIST DROP COLUMN T20_TOT;
QUIT;

##########################################################STEP2: 6개월 이내 2번 약물 
1) 약물 처방 횟수 분포 확인, 1번만 처방 받은 경우 제외 
2) GROUP; NUM 추가 -ROW_NUMBER 대신 MONOTONIC 사용
3) TWICE, ONCE 
4) twice in six month 
####################################################################################################################

#1)약물 처방 횟수 분포 확인 & 1번만 처방 받은 경우 제외 
PROC SQL;
CREATE TABLE TEST AS 
select ID, COUNT(KEY) AS CNT FROM GROUP  GROUP BY ID;

CREATE TABLE GROUP_V1 AS 
SELECT distinct * FROM GROUP A 
INNER JOIN TEST B 
ON A.ID= B.ID;
QUIT;

#2) GROUP; NUM 추가 -ROW_NUMBER 대신 MONOTONIC 사용
PROC SORT data = group_v1 out = SORTDATA;
by ID DATE ;
RUN;

proc sql;
create table group_V2 as 
select DISTINCT a.*,  COUNT(b.m) as row 
from (select *, monotonic() as m from SORTDATA) a
left join 
(select *, monotonic() as m from SORTDATA) b 
on a.ID = b.ID and b.m <= a.m 
group by A.ID, A.DATE
ORDER BY a.ID,row; 
QUIT;

PROC SQL;
DROP TABLE GROUP;
DROP TABLE TEST;
quit;

#3) TWICE, once, group

proc sql;
create table twice as
select distinct ID, DATE AS START  from group_v2 where row =2;

create table once as 
select distinct ID, DATE, datesix  from group_v2 where row =1;

create table group as 
select distinct A.ID, A.START, B.DATE AS FIRST , B.datesix from twice a 
inner join once B on a.ID = b.ID AND A.START <= b.datesix
order by  A.ID,A.DATE;
QUIT;

PROC SQL;
DROP TABLE ROS.GROUP_V1;
quit;

#4) twice in six month 
PROC SQL;
CREATE TABLE GROUP_V3 AS 
SELECT DISTINCT A.*, B.START from GROUP_V2 A
INNER JOIN GROUP B 
ON a.ID = b.ID 
WHERE A.row>=2
order by  A.ID,A.DATE;

ALTER TABLE roS.GROUP_V3 DROP COLUMN m, datesix;
quit;

proc print 
select count(distinct ID) AS CNT FROM GROUP_v2
union all 
select count(distinct ID) AS CNT FROM GROUP_v3;

PROC SQL;
DROP TABLE GROUP;
DROP TABLE ONCE;
DROP TABLE TWICE;
DROP TABLE SORTDATA;
DROP TABLE GROUP_V2;
QUIT;

##########################################################STEP 3: last time  
1) drop date(last prescription + 7 days  + prescription duration)/ last date/death date
4) min date = end date 
5) death; if death_date = < drop_date 
####################################################################################################################
#1) drop date, last date, death date 
proc sql;
create table DROP AS 
select ID, ROS, DATE, T20_TOT,
 (date+7+ TOT_PRES_DD_CNT) AS drop_date format = yymmdd8.
from group_V3 where CNT= row;

ALTER TABLE GROUP_V2 ADD drop_date format=yymmdd8.;
update group_v3 set drop_date = (select input(put(b.drop_date,8.), yymmdd8.) 
	FROM DROP b WHERE A.ID = b.ID);

alter table group_v3 add last_date date format=yymmdd8.;
UPDATE GROUP_V3 
	SET last_date = (input(put(20191231,8.), yymmdd8.));
	
alter table group_v3 add death_date date format=yymmdd8.;

update group_v3 A
	set death_date = (select input(put(b.DTH_YM,8.), yymmdd8.) 
	FROM DTH b WHERE A.ID = b.ID);
	
QUIT;

#2) MIN DATE --UNPIVOT
data want/view=want;
set group_v3;
array vars drop_date last_date death_date;
do _t = 1 to dim(vars);
	if not missing(vars[_t]) then do;
	col1=vname(vars[_t]);
	col2=vars[_t];
	format col2 yymmdd8.;
	output;

	end;
end;
drop  drop_date last_date death_date _t;

proc sql;
CREATE TABLE WANT2 AS 
SELECT DISTINCT ID,min(col2) as end_date format yymmdd8.  
from want
group by ID; 

ALTER TABLE GROUP_V3 ADD END_DATE DATE FORMAT=YYMMDD8.;
UPDATE GROUP_V4 A SET END_DATE = (SELECT end_date from WANT2 b where A.ID=b.ID);
QUIT;


#3) add death column
data want;
set group_v3;
array [*] _date_;
do i=1 to dim(ace);
if ace[i]=. then ace[i]=0;
end;
run;

data group_v3;
set group_v3;
if death_date='.' then death_date=(input(put(99991231,8.), yymmdd8.));
format death_date yymmdd8.;
run;
----
proc sql;
ALTER TABLE  GROUP_V3 ADD DEATH INT; 
UPDATE GROUP_V4  SET DEATH=0;
UPDATE GROUP_V4  SET DEATH=1 WHERE death_date <= drop_date; 
proc sql;
alter table group_v3 drop column death_date, drop_date, last_date;
#alter table LIB_ros.group_v3 drop column drop_date;
#alter table LIB_ros.group_v3 drop column last_date;
drop view WANT;
DROP TABLE WANT2;
quit;

##########################################################STEP 4: add gender, age   
data want/view=want;
set BFC;
AGE = STND_Y-BYEAR+1; 
KEEP INDI_DSCM_NO SEX AGE;
proc sql;
	CREATE TABLE WANTS2 AS 
SELECT DISTINCT * FROM WANT;
	ALTER TABLE LIB_ROS.GROUP_V4 ADD SEX NUM;
UPDATE LIB_ROS.GROUP_V4 A SET SEX = (SELECT SEX FROM WANTS2 B  WHERE A.ID = B.INDI_DSCM_NO);
	ALTER TABLE LIB_ROS.GROUP_V4 ADD AGE NUM;
UPDATE LIB_ROS.GROUP_V4 A SET AGE = (SELECT AGE FROM WANTS2 B  WHERE A.ID = B.INDI_DSCM_NO);
DROP TABLE WANTS2;
DROP VIEW WANT;
DROP TABLE BFC;
##########################################################STEP 5:FOR CCI -DIAGNOSE HISTORY##################  
1) t20, t40: combind multiple dataset by macro 
2) extract diagnose history
################################################################################################
#) MAKE DUMMY DATASET 
data REQ000046708_T20_201401;
input key $ grade@@;
cards;
111 1 222 2 333 3 222 2
; run;
data REQ000046708_T20_201402;
input key $ grade@@;
cards;
113 1 2222 2 3323 3 2232 2
; run;
data REQ000046708_T20_201403;
input key $ grade@@;
cards;
112 1 221 2 331 3 222 2
; run;
data REQ000046708_T20_201404;
input key $ grade@@;
cards;
1211 1 2322 2 3133 3 2222 2
; run;
data REQ000046708_T20_201405;
input key $ grade@@;
cards;
1111 1 2223 2 3332 3 2221 2
; run;
data REQ000046708_T20_201406;
input key $ grade@@;
cards;
2 1 3 2 4 3 5 2
; run;
data REQ000046708_T20_201407;
input key $ grade@@;
cards;
1111 1 2223 2 3332 3 2221 2
; run;
data REQ000046708_T20_201408;
input key $ grade@@;
cards;
1111 1 2223 2 3332 3 2221 2
; run;
data REQ000046708_T20_201409;
input key $ grade@@;
cards;
1818 1 1818 2 1818 3 1818 2
; run;
data REQ000046708_T20_201410;
input key $ grade@@;
cards;
112 1 221 2 331 3 222 2
; run;
data REQ000046708_T20_201411;
input key $ grade@@;
cards;
1211 1 2322 2 3133 3 2222 2
; run;
data REQ000046708_T20_201412;
input key $ grade@@;
cards;
9999 1 9999 2 9999 3 9999 2
; run;
%do yr= 2013 %to 2014; 
#1)T20, T40 macro loop combind datasets
-----
%MACRO yearmonth;
data m1;
set 
%do i= 0 %to 0;
%do qr=1 %to 9;
	REQ000046708_T20_2014&i&qr
%end;
%end;
;
run;
data m2;
set 
%do qr=10 %to 12;
	REQ000046708_T20_2014&qr
%end;
;
run;

data m3;
set m1 m2;
run;
PROC SQL;
drop table M1;
drop table M2;
QUIT;
%MEND;
%yearmonth;

#2) extract diagnose history (1 years ago from index_Date(2nd prescription))

# GROUP_V3: add 1year(past) column
proc sql;
alter table group_V3 add past date format=YYMMDD8.;
update ros.group_V 3 a set past = INTNX('years',A.start_date, -1, 's'); 

# ADD DATE FORMAT COLUMN 
PROC SQL;
ALTER TABLE ROS.T20 ADD datef DATE FORMAT=YYMMDD8.;
UPDATE ROS.T20 A SET datef = input(put( A.MDCARE_ST_RT_DT ,8.), yymmdd8.);

# JOIN: T20 + GROUP_V3  
PROC SQL;
create table ros.sick as 
SELECT DISTINCT A.INDI_DSCM_NO, A.start_date, B.CMN_KEY as key, B.datef,  B.SICK_SYM1 as s1, B.SICK_sYM2 as s2 
FROM  ROS.GROUP_V3 A
INNER JOIN ROS.T20 B
ON A.INDI_DSCM_NO = B.INDI_DSCM_NO
WHERE a.past <= b.datef and b.datef < A.start_date;
QUIT;

# JOIN : + T40
PROC SQL;
CREATE TABLE ROS.SICK2 AS 
SELECT DISTINCT A.*, B.MCEX_SICK_SYM AS s3 
FROM ROS.SICK A 
LEFT JOIN T40 B
ON A.CMN_KEY = B.CMN_KEY; 
QUIT;

##dont erase sick, use agin for key 
# UNPIVOT --TURN COLUMNS TO ROWS
data want/view=want;
set ros.sick2;
array vars s1 s2 s3;
do _t = 1 to dim(vars);
	if not missing(vars[_t]) then do;
	col1=vname(vars[_t]);
	col2=vars[_t];
	output;
	end;
end;

PROC SQL;
create table ros.cci as 
select distinct INDI_DSCM_NO, col2 as sick from want 
ORDER BY INDI_DSCM_NO;
quit;

proc sql;
DROP VIEW WANT;
QUIT;

##########################################################STEP 7: FOR QT SCORE AND QT DRUG ##################  
1) extract past cmn_key --up
2) t30, t60 combind multiple dataset by macro--up 
3) DATA LIST 
4) QT CATEGORY
category1 ; PR/CR/KR 
category2 ; type23 category count
5) COUNT QT CATEGORY 
################################################################################################
3) DATA LIST 
proc sql; 
create TABLE DRUG1 AS
SELECT DISTINCT A.INDI_DSCM_NO, A.key, B.MCARE_DIV_CD_ADJ AS DRUG 
FROM ROS.SICK A 
INNER JOIN ROS.T60 B 
ON A.key = B.CMN_KEY 
ORDER BY A.INDI_DSCM_NO; 

CREATE TABLE DRUG2 AS 
SELECT DISTINCT A.INDI_DSCM_NO, A.key, B.MCARE_DIV_CD_ADJ AS DRUG 
FROM ROS.SICK A 
INNER JOIN ROS.T30 B 
ON A.key = B.CMN_KEY 
ORDER BY A.INDI_DSCM_NO; 

CREATE DRUG AS 
SELECT DISTINCT * FROM DRUG1
UNION ALL 
SELECT DISTINCT * FROM DRUG2;
QUIT;

PROC SQL;
DROP TABLE DRUG1;
DROP TABLE DRUG2;
QUIT;

4) QT CATEGORY 

PROC SQL;
ALTER TABLE DRUG ADD QT CHARACTER;
UPDATE DRUG A SET QT = 
CASE 
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code AND risk='pr')) THEN 'pr'    
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and risk = 'kr')) then 'kr'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and risk = 'cr')) then 'cr'
ELSE ''
end;


PROC SQL;
ALTER TABLE DRUG ADD category CHARACTER;
UPDATE DRUG A SET category = 
CASE 
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code AND test='anticonvulsants')) THEN 'Q1'    
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Analgesic')) then 'Q2'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Antipsychotics')) then 'Q3'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code AND test='Antidementia')) THEN 'Q4'    
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Antispasmodic')) then 'Q5'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Antiarrhythmic')) then 'Q6'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code AND test='Diuretic')) THEN 'Q7'    
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Antihypertensive')) then 'Q8'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Antianginal')) then 'Q9'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code AND test='Antiulcer treatment')) THEN 'Q10'    
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Antiemetics')) then 'Q11'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Antidiarrheal')) then 'Q12'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code AND test='Gastrointestinal Promotility')) THEN 'Q13'    
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Vasoconstrictor')) then 'Q14'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Somatostatin analog')) then 'Q15'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code AND test='Anti Bladder spasm')) THEN 'Q16'    
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Vasodilator ')) then 'Q17'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'antibiotics')) then 'Q18'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Antituberculous')) then 'Q19'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'HIV antiretrovirals')) then 'Q20'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code AND test='Antifungal')) THEN 'Q21'    
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'Antimallarials')) then 'Q22'
WHEN (exists(select distinct code from QT B WHERE A.DRUG= B.code and test = 'opioid')) then 'Q23'
ELSE ''
end;

5) COUNT QT CATEGORY 
PROC SQL;
alter table ROS.GROUP_V3 add PR INT;
UPDATE ROS.GROUP_v3 A SET  PR 
= (SELECT SUM(CASE WHEN QT='pr' THEN 1 ELSE 0 END ) 
FROM DRUG B WHERE A.INDI_DSCM_NO =B.INDI_DSCM_NO  GROUP BY INDI_DSCM_NO); 

alter table ROS.GROUP_V3 add KR INT;
UPDATE ROS.GROUP_v3 A SET  KR 
= (SELECT SUM(CASE WHEN QT='kr' THEN 1 ELSE 0 END ) 
FROM DRUG B WHERE A.INDI_DSCM_NO =B.INDI_DSCM_NO  GROUP BY INDI_DSCM_NO); 

alter table ROS.GROUP_V3 add CR INT;
UPDATE ROS.GROUP_v3 A SET  CR 
= (SELECT SUM(CASE WHEN QT='cr' THEN 1 ELSE 0 END ) 
FROM DRUG B WHERE A.INDI_DSCM_NO =B.INDI_DSCM_NO  GROUP BY INDI_DSCM_NO); 

ALTER TABLE ROS.GROUP_v3 ADD Q1 INT;
UPDATE ROS.GROUP_v3 A SET Q1 
= (SELECT SUM(CASE WHEN category ='Q1' THEN 1 ELSE 0 END ) 
FROM DRUG B WHERE A.INDI_DSCM_NO =B.INDI_DSCM_NO  GROUP BY INDI_DSCM_NO); 

6) CHECK
PROC SQL;
SELECT MEAN(PR)AS PR, MEAN(KR) AS KR , MEAN(CR) AS CR, MEAN(Q1) AS Q1DRUG FROM ROS.GROUP_V3;



