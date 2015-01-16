package KBR::DNA;
# 
# Generic DNA functions
# 
# 
# Kim Brugger (16 Jan 2015), contact: kbr@brugger.dk


use strict;
use warnings;
use Data::Dumper;



# 
# 
# 
# Kim Brugger (16 Jan 2015)
sub compliment {
  my ( $seq ) = @_;

  $seq =~ tr/[ACGT]/[TGCA]/;

  $seq = reverse( $seq );
      
  return $seq;
}



1;



