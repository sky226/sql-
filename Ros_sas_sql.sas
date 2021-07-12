## 파일 업로드 
PROC IMPORT datafile='/folders/myfolders/ROS/BFC_100.csv'
dbms=csv 
out=BFC;
GETNAMES = Yes;
RUN;

############################################# STEP1: 데이터 전처리   
1) 30T, 60T 합치기-T3060 (KEY, DRUG, TOT)
2)LIST: ROS/ATO ID, KEY, DRUG, TOT 
3)ROSATO: ROS, ATO JOIN 겹치는 ID 확인
4) GROUP: 순수 ROS, ATO 찾기(P_ROS/ATO): ROS- ROSATO/ ATO- ROSATO (ID, KEY, 요양개시일자, DRUG, TOT)
###########################################################################################################
#1 T3060 TABLE 합치기 
PROC SQL;
CREATE TABLE ROS.T3060 AS
SELECT * FROM ROS.T30
UNION ALL 
SELECT * FROM ROS.T60;
QUIT;

#2 list; T20, T60 JOIN(ID, KEY, DRUG, TOT )-> ROS, ATO LIST 

PROC SQL; 
CREATE TABLE LIST AS 
SELECT distinct T20.INDI_DSCM_NO, T60.CMN_KEY, T20.MDCARE_ST_RT_DT, T60.MCARE_DIV_CD_ADJ, T20.TOT_PRES_DD_CNT
FROM T60  T60 
INNER JOIN T20 T20 
ON 
   ( T60.CMN_KEY = T20.CMN_KEY ) 
ORDER BY T20.INDI_DSCM_NO ASC; 

CREATE TABLE ros AS 
SELECT * 
FROM LIST 
WHERE MCARE_DIV_CD_ADJ = 4540;

CREATE TABLE ATO AS 
SELECT * 
FROM LIST 
WHERE MCARE_DIV_CD_ADJ = 1115;
QUIT;


#3 ROSATO: ros, ato 둘다 복용 ID (ID, KEY, DRUG, TOT )
PROC SQL; 
CREATE TABLE ROSATO
AS 
SELECT DISTINCT R.INDI_DSCM_NO
FROM ROS R 
INNER JOIN ATO A 
ON 
   ( R.INDI_DSCM_NO = A.INDI_DSCM_NO ) 
ORDER BY R.INDI_DSCM_NO; 
QUIT;

#4 GROUP: 순수 ROS, ATO 찾기(P_ROS/ATO): ROS- ROSATO/ ATO- ROSATO (ID, KEY, 요양개시일자, DRUG, TOT): UPDATAE

PROC SQL;
CREATE TABLE pr AS 
SELECT DISTINCT *
FROM ros R 
LEFT JOIN  ROSATO RA
ON R.INDI_DSCM_NO= RA.INDI_DSCM_NO
WHERE RA.INDI_DSCM_NO IS NULL; 

CREATE TABLE pA AS 
SELECT DISTINCT *
FROM ATO R 
LEFT JOIN  ROSATO RA
ON R.INDI_DSCM_NO= RA.INDI_DSCM_NO
WHERE RA.INDI_DSCM_NO IS NULL; 

create table ROS.GROUP AS 
SELECT DISTINCT * FROM PA 
UNION ALL 
SELECT DISTINCT * FROM PR;

ALTER TABLE ROS.GROUP ADD ROS INT ;

UPDATE ROS.GROUP 
	SET ROS = 1 
	WHERE MCARE_DIV_CD_ADJ= 4540 ;

UPDATE ROS.GROUP 
	SET ROS = 0 
	WHERE MCARE_DIV_CD_ADJ= 1115 ;

alter table ros.group drop column  MCARE_DIV_CD_ADJ;

DROP TABLE ATO;
DROP TABLE ROS;
DROP TABLE ROSATO;
DROP TABLE PA;
DROP TABLE PR;
DROP TABLE LIST;
DROP TABLE T20;
DROP TABLE T60;
quit;

##########################################################STEP2: 6개월 이내 2번 약물 
1) 약물 처방 횟수 분포 확인, 1번만 처방 받은 경우 제외 
2) GROUP; NUM 추가 -ROW_NUMBER 대신 MONOTONIC 사용
3) TWICE, ONCE 
4) twice in six month 
####################################################################################################################

#1)약물 처방 횟수 분포 확인 & 1번만 처방 받은 경우 제외 
PROC SQL;
CREATE TABLE TEST AS 
select INDI_DSCM_NO, COUNT(CMN_KEY) AS CNT FROM ROS.GROUP  GROUP BY INDI_DSCM_NO;

CREATE TABLE OVER AS 
SELECT INDI_DSCM_NO, CNT FROM TEST WHERE CNT >1;

CREATE TABLE ROS.GROUP_V1 AS 
SELECT distinct * FROM ROS.GROUP A 
INNER JOIN OVER B 
ON A.INDI_DSCM_NO= B.INDI_DSCM_NO;
QUIT;

#2) GROUP; NUM 추가 -ROW_NUMBER 대신 MONOTONIC 사용
PROC SORT data = ros.group_v1 out = SORTDATA;
by INDI_DSCM_NO MDCARE_ST_RT_DT;
RUN;

proc sql;
create table ros.group_row as 
select DISTINCT a.INDI_DSCM_NO, a.CMN_KEY,a.MDCARE_ST_RT_DT,a.TOT_PRES_DD_CNT,a.ROS,a.CNT, COUNT(b.m) as row 
from (select *, monotonic() as m from SORTDATA) a
left join 
(select *, monotonic() as m from SORTDATA) b 
on a.INDI_DSCM_NO = b.INDI_DSCM_NO and b.m <= a.m 
group by a.INDI_DSCM_NO, a.MDCARE_ST_RT_DT 
ORDER BY a.INDI_DSCM_NO,row; 

DROP TABLE GROUP;
DROP TABLE OVER;
DROP TABLE TEST;
quit;

#3) 날짜형으로 변경, TWICE, once, group
data ros.group_v2;
set ros.group_row;
date = input(put(MDCARE_ST_RT_DT,8.), yymmdd8.);
datesix= date+180;
format date yymmdd8.;
format datesix yymmdd8.;
run;

proc sql;
create table twice as
select distinct INDI_DSCM_NO, date from ros.group_v2 where row =2;

create table once as 
select distinct INDI_DSCM_NO, date,  datesix from ros.group_v2 where row =1;

create table group as 
select distinct A.INDI_DSCM_NO, A.date , B.DATE AS FIRST , B.datesix from twice a 
inner join once B on A.INDI_DSCM_NO =B.INDI_DSCM_NO AND A.date <= b.datesix
order by  A.INDI_DSCM_NO,A.date;

DROP TABLE ROS.GROUP_V1;
DROP TABLE ROS.GROUP_V2;
DROP TABLE ROS.GROUP;
quit;

#4) twice in six month 
PROC SQL;
CREATE TABLE ros.GROUP_V3 AS 
SELECT DISTINCT A.*, B.date as start_date from ROS.GROUP_ROW A
INNER JOIN GROUP B 
ON A.INDI_DSCM_NO = B.INDI_DSCM_NO
WHERE A.row>=2
ORDER BY INDI_DSCM_NO, date;

ALTER TABLE roS.GROUP_V3 DROP COLUMN MDCARE_ST_RT_DT, datesix;
quit;

PROC SQL;
DROP TABLE ROS.GROUP_ROW;
DROP TABLE GROUP;
DROP TABLE ONCE;
DROP TABLE TWICE;
DROP TABLE SORTDATA;
QUIT;

##########################################################STEP 3: last time  
1) drop date(last prescription + 7 days  + prescription duration)/ last date/death date
4) min date = end date 
5) death; if death_date = < drop_date 
####################################################################################################################
#1) drop date, last date, death date 
proc sql;
create table LIB_ROS.GROUP_V4 AS 
select INDI_DSCM_NO AS ID, ROS, start_date,
 (date+7+ TOT_PRES_DD_CNT) AS drop_date format = yymmdd8.
from lib_ros.group_V3 where CNT= row;

drop table LIB_ROS.GROUP_V3;

alter table lib_ros.group_v4 add last_date date format=yymmdd8.;

UPDATE LIB_ROS.GROUP_V4 
	SET last_date = (input(put(20191231,8.), yymmdd8.));
	
alter table lib_ros.group_v4 add death_date date format=yymmdd8.;

update lib_ros.group_v4 A
	set death_date = (select input(put(b.DTH_YM,8.), yymmdd8.) 
	FROM DTH b WHERE A.ID = b.INDI_DSCM_NO);
	
QUIT;

#4) MIN DATE 
data want/view=want;
set lib_ros.group_v4;
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

ALTER TABLE LIB_ROS.GROUP_V4 ADD END_DATE DATE FORMAT=YYMMDD8.;
UPDATE LIB_ROS.GROUP_V4 A SET END_DATE = (SELECT end_date from WANT2 b where A.ID=b.ID);
#5) add death column
data want;
set lib_ros.group_v4;
array [*] _date_;
do i=1 to dim(ace);
if ace[i]=. then ace[i]=0;
end;
run;

data lib_Ros.group_v4;
set lib_ros.group_v4;
if death_date='.' then death_date=(input(put(99991231,8.), yymmdd8.));
format death_date yymmdd8.;
run;

proc sql;
ALTER TABLE  LIB_ROS.GROUP_V4 ADD DEATH INT; 
UPDATE LIB_ROS.GROUP_V4  SET DEATH=0;
UPDATE LIB_ROS.GROUP_V4  SET DEATH=1 WHERE death_date <= drop_date; 
proc sql;
alter table LIB_ros.group_v4 drop column death_date;
alter table LIB_ros.group_v4 drop column drop_date;
alter table LIB_ros.group_v4 drop column last_date;
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
1) combind multiple dataset by macro 
2) extract diagnose history
################################################################################################
#)MAKE DUMMY DATASET 
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

#1) macro loop combind datasets
%macro yearmonth;
data m1;
set 
%do yr= 2013 %to 2014; 
%do i= 0 %to 0;
%do qr=1 %to 9;
	REQ000046708_T20_&yr&i&qr
%end;
%end;
%end;
;
run;
data m2;
set 
%do yr= 2013 %to 2014; 
%do qr=10 %to 12;
	REQ000046708_T20_&yr&qr
%end;
%end;
;
run;

data m3;
set m1 m2;
run;
PROC SQL;
drop table M1;
drop table M2;
%mend;

%month;

#2) extract diagnose history (1 years ago from index_Date(2nd prescription))
# add 1year(past) column
proc sql;
alter table ros.group_V3 add past date format=YYMMDD8.;
update ros.group_V3 a set past = INTNX('years',A.start_date, -1, 's'); 

# ADD DATE FORMAT COLUMN 
PROC SQL;
ALTER TABLE ROS.T20 ADD datef DATE FORMAT=YYMMDD8.;
UPDATE ROS.T20 A SET datef = input(put( A.MDCARE_ST_RT_DT ,8.), yymmdd8.);

# JOIN 

PROC SQL;
create table ros.sick as 
SELECT DISTINCT A.INDI_DSCM_NO, A.start_date, B.CMN_KEY as key, B.datef,  B.SICK_SYM1 as s1, B.SICK_sYM2 as s2 
FROM  ROS.GROUP_V3 A
INNER JOIN ROS.T20 B
ON A.INDI_DSCM_NO = B.INDI_DSCM_NO
WHERE a.past <= b.datef and b.datef < A.start_date;

data want/view=want;
set ros.sick;
array vars s1 s2;
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

DROP VIEW WANT;
QUIT;

##########################################################STEP 7: FOR QT SCORE AND QT DRUG ##################  
1) extract past cmn_key --up
2) combind multiple dataset by macro--up 
3) DATA LIST 
4) QT CATEGORY
category1 ; PR/CR/KR 
category2 ; type23 category count
5) COUNT QT CATEGORY 
################################################################################################
3) DATA LIST 
proc sql; 
create TABLE DRUG AS
SELECT DISTINCT A.INDI_DSCM_NO, A.key, B.MCARE_DIV_CD_ADJ AS DRUG 
FROM ROS.SICK A 
INNER JOIN ROS.T60 B 
ON A.key = B.CMN_KEY 
ORDER BY A.INDI_DSCM_NO; 

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



