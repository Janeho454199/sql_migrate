######################################################################
# BTEQ script in Perl, generate by Script Wizard
# Date Time    : 2005-09-15 18:08:02
# Target Table : CARD_ACCT_RELA
# Script File  : CARD_ACCT_RELA_tcrm0200.pl
# Author       : MQK
# Modified by  : Tony 2006-6-12
######################################################################

use strict; # Declare using Perl strict syntax
use File::Basename;
use Cwd 'abs_path';

#
# If you are using other Perl's package, declare here
#

######################################################################
# Variable Section
my $AUTO_HOME = $ENV{"AUTO_HOME"};
my $AUTO_DATA = "${AUTO_HOME}/DATA";
my $AUTO_LOG  = "${AUTO_HOME}/LOG";
my $LOGDIR  = "";
my $CRMTARGETDB = $ENV{"AUTO_CRMMARTDB"};
my $CRMSOURCEDB = $ENV{"AUTO_DATADB2"};
my $TEMPDB = $ENV{"AUTO_TEMPDB"};
my $LOGDB = $ENV{"AUTO_LOGDB"};

my $ODSDB = $ENV{"AUTO_ODSDB"};
my $AUTO_ENV = $ENV{"AUTO_ENV"};
if ( !defined($AUTO_ENV)) {
   $AUTO_ENV = substr($CRMTARGETDB,0,1);
} else {
   $AUTO_ENV = substr($AUTO_ENV,0,1);
}
my $MAXDATE = $ENV{"AUTO_MAXDATE"};
if ( !defined($MAXDATE) ) {
   $MAXDATE = "21001231";
}
my $NULLDATE = $ENV{"AUTO_NULLDATE"};
if ( !defined($NULLDATE) ) {
   $NULLDATE = "19000101";
}
my $ERRDATE = $ENV{"AUTO_ERRDATE"};
if ( !defined($ERRDATE) ) {
   $ERRDATE = "19000102";
}
my $TXNDATE;
my $ETLBIN = abs_path(dirname("$0"));
my $ETLAPP = substr($ETLBIN,0,length($ETLBIN)-4);
my $SUBSYS = substr($ETLAPP,length($AUTO_HOME)+5,3);
my $DDL = "${ETLAPP}/ddl";
my $LOGON_STR;
my $LOGON_FILE = "${AUTO_HOME}/etc/ETL_LOGON";
my $CONTROL_FILE;

my $SCRIPT = basename("$0");

######################################################################
# BTEQ function
sub run_bteq_command
{
   my $rc = open(BTEQ, "| bteq");

   # To see if bteq command invoke ok?
   unless ($rc) {
      print "Could not invoke BTEQ command\n";
      return -1;
   }

   ### Below are BTEQ scripts ###
   print BTEQ <<ENDOFINPUT;

  --(1/1).加载目标表:CARD_ACCT_RELA(卡帐关系表)
  --考察点:支持重跑
DELETE FROM ${CRMTARGETDB}.CARD_ACCT_RELA WHERE DATA_DT = CAST('${TX_DATE}' AS DATE FORMAT 'YYYYMMDD');

INSERT INTO ${CRMTARGETDB}.CARD_ACCT_RELA(
    ACT_IDN_SKY, --帐号
    CARDID, --基准卡卡号
    ACT_IDN_SKY_HOST, --主关联帐号
    PARTYID, --当事人编号
    HOST_PARTYID, --主卡当事人编号
    FX_RTE, --汇率
    SUM_FLAG, --汇总标志
    STT_DTE, --生成日期
    STD_DTE, --结束日期
)
SELECT COALESCE(GL2.ACT_IDN_SKY, ''),
       COALESCE(GL2.BM_CRD_NBR, ''),
       COALESCE(GL2.ACT_IDN_SKY1, ''),
       COALESCE(GL2.CSR_IDN_SKY, ''),
       CASE
           WHEN GL2.ACT_CGY_CDE = '1' THEN GL2.CSR_IDN_SKY
           ELSE GL2.CSR_IDN_SKY
           END,
       COALESCE(GL2.FX_RTE / 100, ''),
       CASE
           WHEN (GL2.ACT_CGY_CDE = '' and GL2.MAN_SPY_IND = '') THEN '0'
           ELSE ''
           END,
       COALESCE(GL2.STT_DTE / 100, ''),
       COALESCE(GL2.STD_DTE / 100, ''),
FROM (
         SELECT FL3.ACT_IDN_SKY,
                FL3.BM_CRD_NBR,
                FL3.ACT_IDN_SKY1,
                FL3.CSR_IDN_SKY,
                FL3.ACT_CGY_CDE,
                FL3.FX_RTE,
                FL3.ACT_CGY_CDE,
                FL3.STT_DTE,
                FL3.STD_DTE
         FROM
             -- 考察点：CAST函数
             (
                 SELECT ACT_IDN_SKY,
                        BM_CRD_NBR,
                        ACT_IDN_SKY1,
                        CSR_IDN_SKY,
                        ACT_CGY_CDE,
                        FX_RTE,
                        ACT_CGY_CDE,
                        CAST(STT_DTE AS DATE FORMAT 'YYYYMMDD'),
                        CAST(STD_DTE AS DATE FORMAT 'YYYYMMDD')
                 FROM ${CRMSOURCEDB}.party_acct_rela_h
                 WHERE ACT_CGY_CDE = '1'
                   -- 考察点：EXCTRACT函数
                   AND (EXTRACT(YEAR FROM STD_DTE + 1) - EXTRACT(YEAR FROM STT_DTE - 1)) > 0
             ) FL3
     ) GL2;

   ### End of BTEQ scripts ###
   close(BTEQ);

   my $RET_CODE = $? >> 8;

   # if the return code is 12, that means something error happen
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
   open(LOGONFILE_H, "${LOGON_FILE}");
   $LOGON_STR = <LOGONFILE_H>;
   close(LOGONFILE_H);

   # Get the decoded logon string
   $LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   my $today = "${year}${mon}${mday}";

   # Call bteq command to load data
   $ret = run_bteq_command();

   print "run_bteq_command() = $ret";
   return $ret;
}

######################################################################
# program section

# To see if there is one parameter,
# if there is no parameter, exit program
if ( $#ARGV < 0 ) {
   print "Usage: ".basename($0)."  Control_File \n";
   print "        Control_File: dir.jobnameYYYYMMDD (or sysname_jobname_YYYYMMDD.dir)\n";
   exit(1);
}

# Get the first argument
$CONTROL_FILE = $ARGV[0];

if ($CONTROL_FILE =~/[0-9]{8}($|\.)/) {
   $TXNDATE = substr($&,0,8);
}

open(STDERR, ">&STDOUT");

my $ret = main();

exit($ret);
