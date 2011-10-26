#!/bin/bash

function usage
{
    echo "usage: makeRules.sh"
    echo ""
    echo "options:"
    echo "-odir        : output directory (default: ./rules)"
    echo ""
    echo "required defined env variables:"
    echo "HPCYK      : path to the tools"
    echo "PATCNF     : pattern-based rule set config file"
    echo "PPI        : directory with phrase pair inventory (regular phrases)"
    echo "HPPI       : directory with hierarchical rules by pattern"
    echo "LEXS2T     : Source-to-target Translation Table (Giza-format) for lexical features"
    echo "LEXT2S     : Target-to-source Translation Table (Giza-format) for lexical features"
    echo ""
    echo "optional env variables:"
    echo "TTOSCTRAINTS: if set, ascii constraints are included in rulefile"
    echo "IHOLD      : jobs id's of preceding jobs to this script (qsub -hold_jid $METHOLD) (def:none)"
    exit 1
}

## Check environment variables: 
if [ -z $HPCYK ] ; then
  echo "Environment variable HPCYK must be specified!" > /dev/stderr ; usage;
fi
if [ -z $PATCNF ] ; then
  echo "Environment variable PATCNF must be specified!" > /dev/stderr ; usage;
fi
if [[ -z $PPI || -z $HPPI ]] ; then
  echo "Environment variables PPI and HPPI must be specified!" > /dev/stderr ; usage;
fi
if [[ -z $LEXS2T || ! -r $LEXT2S ]] ; then
  echo "[makeRules.sh] Environment variables LEXS2T and LEXT2S must be defined (need for lexical features)"; exit;
fi
H0="" ; if [[ ! -z "$IHOLD" && "$IHOLD" != "," ]]; then H0="-hold_jid $IHOLD"; echo "[makeRules.sh] Pending on jid: $H0"; fi

## Variable definitions:
ODIR=rules
scratch=1

## Read optional parameters:
while [ "$1" != "" ]; do

    case $1 in
	-odir )                shift
	                       ODIR=$1
	                       ;;
	-h    )                usage;
                               ;;
        * )                    echo "Unknown option: $1"; usage;
    esac
    shift
done

####################################################################################################################################

mkdir -p err $ODIR
qsub="qsub -j y -e err/err" ; if [ ! -z "$FLAGSQSUB" ] ; then qsub="$qsub $FLAGSQSUB"; fi


### makeRules: builds rule-set
cmd="$qsub $H0 -N rbld -o err/rbld $HPCYK/rulebuilding/makeRules_LOCAL.sh $PPI $HPPI $ODIR"
echo $cmd; IL=`$cmd | awk '{print $3;}' | sed 's:\..*::'`

echo "[makeRules.sh]LASTJID= $IL"
