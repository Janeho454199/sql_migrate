######################################################################
# BTEQ script in Perl, generate by Script Wizard
# Date Time    : 2005-09-15 18:08:02
# Target Table : CARD_ACCT_RELA
# Script File  : CARD_ACCT_RELA_tcrm0200.pl
# Author       : MQK
# Modified by  : Tony 2006-6-12
######################################################################

use strict; #
Declare using Perl strict syntax
use File::Basename;
use
Cwd ''abs_path'';

#
# If you are using other Perl''s package, declare
here
#

######################################################################
# Variable Section
my $AUTO_HOME = $ENV{"AUTO_HOME"};
my
$AUTO_DATA = "${AUTO_HOME}/DATA";
my
$AUTO_LOG  = "${AUTO_HOME}/LOG";
my
$LOGDIR  = "";
my
$CRMTARGETDB = $ENV{"AUTO_CRMMARTDB"};
my
$CRMSOURCEDB = $ENV{"AUTO_DATADB2"};
my
$TEMPDB = $ENV{"AUTO_TEMPDB"};
my
$LOGDB = $ENV{"AUTO_LOGDB"};

my
$ODSDB = $ENV{"AUTO_ODSDB"};
my
$AUTO_ENV = $ENV{"AUTO_ENV"};
if
( !defined($AUTO_ENV)) {
   $AUTO_ENV = substr($CRMTARGETDB,0,1);
} else {
   $AUTO_ENV = substr($AUTO_ENV,0,1);
}
my $MAXDATE = $ENV{"AUTO_MAXDATE"};
if
( !defined($MAXDATE) ) {
   $MAXDATE = "21001231";
}
my $NULLDATE = $ENV{"AUTO_NULLDATE"};
if
( !defined($NULLDATE) ) {
   $NULLDATE = "19000101";
}
my $ERRDATE = $ENV{"AUTO_ERRDATE"};
if
( !defined($ERRDATE) ) {
   $ERRDATE = "19000102";
}
my $TXNDATE;
my
$ETLBIN = abs_path(dirname("$0"));
my
$ETLAPP = substr($ETLBIN,0,length($ETLBIN)-4);
my
$SUBSYS = substr($ETLAPP,length($AUTO_HOME)+5,3);
my
$DDL = "${ETLAPP}/ddl";
my
$LOGON_STR;
my
$LOGON_FILE = "${AUTO_HOME}/etc/ETL_LOGON";
my
$CONTROL_FILE;

my
$SCRIPT = basename("$0");

######################################################################
# BTEQ function
sub run_bteq_command
{
   my $rc = open(BTEQ, "| bteq");

   #
To see if bteq command invoke ok?
   unless ($rc) {
      print "Could not invoke BTEQ command\n";
return -1;
}

   ### Below are BTEQ scripts ###
   print BTEQ <<ENDOFINPUT;

${LOGON_STR}

--(1/1).加载目标表:CARD_ACCT_RELA(卡帐关系表)

drop
join index
${CRMTARGETDB}
.
CARD_ACCT_RELA_jdx1;
drop
join index
${CRMTARGETDB}
.
CARD_ACCT_RELA_jdx2;
drop
join index
${CRMTARGETDB}
.
CARD_ACCT_RELA_jdx3;

Delete
From ${CRMTARGETDB}.CARD_ACCT_RELA all;

.IF ERRORCODE <> 0 THEN .QUIT 12;

/* 新数准备 -- 数据清洗*/
insert into ${CRMTARGETDB}.CARD_ACCT_RELA(ACT_IDN_SKY, --帐号
                                          CARDID, --基准卡卡号
                                          ACT_IDN_SKY_HOST, --主关联帐号
                                          PARTYID, --当事人编号
                                          HOST_PARTYID, --主卡当事人编号
                                          FX_RTE, --汇率
                                          SUM_FLAG --汇总标志
)
select COALESCE(AL2.ACT_IDN_SKY, ''''),
       COALESCE(AL2.BM_CRD_NBR, ''''),
       COALESCE(AL2.ACT_IDN_SKY1, ''''),
       COALESCE(AL2.CSR_IDN_SKY, ''''),
       CASE
           when AL2.ACT_CGY_CDE = ''1 '' then AL2.CSR_IDN_SKY
           else fl2.CSR_IDN_SKY
           END,
       COALESCE(GL2.FX_RTE / 100, ''''),
       CASE
           when (AL2.ACT_CGY_CDE = ''3 '' and AL2.MAN_SPY_IND = ''0 '') then ''0 ''
           else ''1 ''
           END
from (select a.ACT_IDN_SKY,
             a.BM_CRD_NBR,
             b.ACT_IDN_SKY act_idn_sky1,
             b.CSR_IDN_SKY,
             b.ACT_CGY_CDE,
             b.MAN_SPY_IND
      from (select ACT_IDN_SKY, BM_CRD_NBR
            from ${CRMSOURCEDB}.T03_ACCT_BMCARD_RELA_H
            where STT_DTE <= ''${TXNDATE}'' and END_DTE >= ''${TXNDATE}''
           ) a
               inner join ${CRMSOURCEDB}.T03_BM_CARD_INFO b
                          on a.BM_CRD_NBR = b.BM_CRD_NBR
     ) AL2
         inner join
     (select ACT_IDN_SKY, CRY_CDE, ACT_CGY_CDE
      from ${CRMSOURCEDB}.T03_ACCOUNT
     ) CL2
     on AL2.ACT_IDN_SKY = CL2.ACT_IDN_SKY
         left join
     (select ACT_IDN_SKY, TOP_ACT_IDN_SKY
      from ${CRMSOURCEDB}.T03_ACCT_STRUCTURE
      group by ACT_IDN_SKY qualify rank(STT_DTE)  = 1
     ) EL2
     on AL2.ACT_IDN_SKY = EL2.ACT_IDN_SKY
         left join
     (select FL3.ACT_IDN_SKY as ACT_IDN_SKY, FL3.CSR_IDN_SKY as CSR_IDN_SKY
      from (select ACT_IDN_SKY, CSR_IDN_SKY, STT_DTE
            from ${CRMSOURCEDB}.party_acct_rela_h
            where CSR_ACT_RLT_CDE = ''1 ''
            group by ACT_IDN_SKY qualify rank(STT_DTE) = 1
           ) FL3
      group by ACT_IDN_SKY qualify rank(CSR_IDN_SKY) = 1
     ) FL2
     on EL2.TOP_ACT_IDN_SKY = FL2.ACT_IDN_SKY
         left join
     (select CRY_CDE2, FX_RTE
      from ${CRMSOURCEDB}.T99_FX_RATE
      where RTE_TYP_CDE = ''DFLT''
			and STT_DTE <= ''${TXNDATE}''
        and END_DTE >= '' ${TXNDATE}''
        and CRY_CDE1 =''156''
     ) GL2
     on CL2.CRY_CDE = GL2.CRY_CDE2
;

.IF ERRORCODE <> 0 THEN .QUIT 12;


create
join index
${CRMTARGETDB}
.
CARD_ACCT_RELA_jdx1,
NO
fallback
as
select ACT_IDN_SKY
     , CARDID
     , ACT_IDN_SKY_HOST
     , PARTYID
     , FX_RTE
     , HOST_PARTYID
     , SUM_FLAG
FROM ${CRMTARGETDB}.CARD_ACCT_RELA PRIMARY INDEX(CARDID)
;
create
join index
${CRMTARGETDB}
.
CARD_ACCT_RELA_jdx2,
NO
fallback
as
select ACT_IDN_SKY
     , CARDID
     , PARTYID
     , SUM_FLAG
FROM ${CRMTARGETDB}.CARD_ACCT_RELA PRIMARY INDEX(PARTYID)
;

create
join index
${CRMTARGETDB}
.
CARD_ACCT_RELA_jdx3,
NO
fallback
as
select ACT_IDN_SKY
     , PARTYID
     , HOST_PARTYID
     , SUM_FLAG
FROM ${CRMTARGETDB}.CARD_ACCT_RELA PRIMARY INDEX(HOST_PARTYID)
;


.LOGOFF;
.QUIT;

ENDOFINPUT

###
End of BTEQ scripts ###
   close(BTEQ);

   my
$RET_CODE = $? >> 8;

   #
if the return code is 12, that means something error happen
   # so we return 1, otherwise, we return 0 means ok
   if ( $RET_CODE == 12 ) {
      return 1;
}
   else {
      return 0;
}
}

######################################################################
# main function
sub main
{
   my $ret;
open (LOGONFILE_H, "${LOGON_FILE}");
$LOGON_STR
= <LOGONFILE_H>;
close (LOGONFILE_H);

#
Get the decoded logon string
   $LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;
   my
($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   $year
+= 1900;
   $mon
= sprintf("%02d", $mon + 1);
   $mday
= sprintf("%02d", $mday);
   my
$today = "${year}${mon}${mday}";

   #
Call bteq command to load data
   $ret = run_bteq_command();

   print
"run_bteq_command() = $ret";
return $ret;
}

######################################################################
# program section

# To see if there is one parameter,
# if there is no parameter, exit program
if ( $#ARGV < 0 ) {
   print "Usage: ".basename($0)."  Control_File \n";
   print
"        Control_File: dir.jobnameYYYYMMDD (or sysname_jobname_YYYYMMDD.dir)\n";
   exit
(1);
}

# Get the first argument
$CONTROL_FILE = $ARGV[0];

if
($CONTROL_FILE =~/[0-9]{8}($|
\.)/) {
   $TXNDATE = substr($&,0,8);
}

open(STDERR, ">&STDOUT");

my
$ret = main();

exit
($ret);
