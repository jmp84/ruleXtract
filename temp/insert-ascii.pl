#!/usr/bin/perl
#$ -S /usr/bin/perl

die "$0 monotone.rulefile ascii.idx\n" if @ARGV!=2;

my $rulefile=$ARGV[0];
my $asciifile=$ARGV[1];

my %hashasciirule;
open ASCIIFILE,"zcat -f $asciifile | " or die "Failed to open $asciifile\n";

while (!eof(ASCIIFILE)) {
    my $asciil=<ASCIIFILE>;
    chomp $asciil;
    die if $asciil!~/^\d+: (.+) # (.+)/;

    ## COMMENTS=Each ascii constraint sequence is added as a single multi-word 'free' rule
    ## but if it has more than 5 words, then it is split into more than one rule!
    my ($SRC,$TRG)=($1,$2); 
    my @SRCs=split " ",$SRC; my @TRGs=split " ",$TRG;
    $jS=@SRCs; $jT=@TRGs;
    die if ($jS!=$jT);
    my ($thisSRC,$thisTRG)=($SRCs[0],$TRGs[0]);

    for (my $k=1;$k<$jS;$k++) {
	if ($k%5==0){
	    print STDOUT "X $thisSRC $thisTRG 0 0 0 0 0 0 0 0 0 0\n";
	    $hashasciirule{"${thisSRC}+${thisTRG}"}=1;
	    $hashasciirule{"${thisSRC}+<dr>"}=1;
	    ($thisSRC,$thisTRG)=($SRCs[$k],$TRGs[$k]);
	    next;
	}
	$thisSRC.="_" . $SRCs[$k];
	$thisTRG.="_" . $TRGs[$k];
    }
    #if ($jS%5){ 
	print STDOUT "X $thisSRC $thisTRG 0 0 0 0 0 0 0 0 0 0\n"; 
	$hashasciirule{"${thisSRC}+${thisTRG}"}=1;
	$hashasciirule{"${thisSRC}+<dr>"}=1;
    #}
}
close ASCIIFILE;

open RULEFILE, "zcat -f $rulefile | " or die "Failed to open $rulefile\n";

## COMMENTS=Also, if a rule existed with the source side of an ascii constraint (translated to sth or to <dr>),
## then we remove that original rule.
while (!eof(RULEFILE)) {
	my $rule=<RULEFILE>;
	die "Incorrect line format" if ($rule!~/(.+?) (.+?) (.+?) /);
	next if (defined $hashasciirule{"$2+$3"});
	print STDOUT $rule;
}


close RULEFILE;
