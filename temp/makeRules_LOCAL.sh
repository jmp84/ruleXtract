#!/bin/bash
#$ -S /bin/bash

hostname 
echo STARTED: `date`

if [ $# -ne 3 ] ; then echo "Usage: $0 inppidir inhppidir outputdir

where:
 inppidir   directory with standard phrases (v.map , utov.prob, vtou.prob, ... files expected)
 inhppidir  directory with hierarchical phrases by patterns (pat.W_X-W-X , pat.X2_W_X1-X1_W_X2 ... files expected)

Required env. variables:
  PATCNF    pattern config file (specifies how to create the rulefile)
  LEXS2T    Source-to-target Translation Table (Giza-format) for lexical features
  LEXT2S    Target-to-source Translation Table (Giza-format) for lexical features

Optional env. variables:
  TTOSCTRAINTS    if set, ascii constraints are included in rulefile!

"; exit 1; fi


if [[ -z $PATCNF || ! -r $PATCNF ]] ; then
  echo "[makeRules.sh] Environment variable PATCNF is not defined or file is not readable!"; exit;
fi
if [[ -z $LEXS2T || ! -r $LEXT2S ]] ; then
  echo "[makeRules.sh] Environment variables LEXS2T and LEXT2S must be defined (need for lexical features)"; exit;
fi

IDIR=$1
IHDIR=$2
TRUEODIR=$3; 
ODIR=/scratch/makerules.`whoami`.$JOB_ID.`date +%H%M%N`
mkdir -p $ODIR

monotoneRules=$ODIR/rules.monotone.dp.gz
if [ ! -e $monotoneRules ] ; then
    echo "[makeRules.sh] Building $monotoneRules"
    $HPCYK/rulebuilding/makeMonotoneRules.pl $IDIR | gzip > $ODIR/rules.monotone.dp.notsorted.gz
    
    echo "S S_X S_X 0 0 0 0 -1 0 0 0 0 0" | gzip >>$ODIR/rules.monotone.dp.notsorted.gz;
    echo "S X X 0 0 0 0 0 0 0 0 0 0" | gzip >>$ODIR/rules.monotone.dp.notsorted.gz;
    echo "X 1 <s>_<s>_<s> 0 0 1 1 0 0 0 1 0 0"| gzip >>$ODIR/rules.monotone.dp.notsorted.gz
    echo "X 2 </s> 0 0 1 1 0 0 0 1 0 0"| gzip >>$ODIR/rules.monotone.dp.notsorted.gz
    echo "X V V 0 0 0 0 0 0 0 0 0 0"| gzip >>$ODIR/rules.monotone.dp.notsorted.gz
    
    if [ ! -z $TTOSCTRAINTS ]; then # Reformat ascii rules
	echo "[makeRules.sh] Adding ascii constraints [$HPCYK/rulebuilding/insert-ascii.pl]"
	$HPCYK/rulebuilding/insert-ascii.pl $ODIR/rules.monotone.dp.notsorted.gz $TTOSCTRAINTS | gzip > $ODIR/rules.monotone.dp.notsorted.ascii.gz
	mv $ODIR/rules.monotone.dp.notsorted.ascii.gz $ODIR/rules.monotone.dp.notsorted.gz
    fi
    
    $HPCYK/rulebuilding/sortRules.pl $ODIR/rules.monotone.dp.notsorted.gz | gzip > $monotoneRules
    rm -f $ODIR/rules.monotone.dp.notsorted.gz
else 
    echo "[makeRules.sh] Monotone Rules already found in: $monotoneRules ... SKIP build"
fi



mkdir -p $TRUEODIR
( cd $ODIR; tar cf - * ) | ( cd $TRUEODIR ; tar xvf - )
rm -rf $ODIR

echo "[makeRules.sh] Done"

echo ENDED: `date`
