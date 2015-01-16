package KBR::File;
# 
# Generic file/fastq/etc IO
# 
# 
# Kim Brugger (16 Jan 2015), contact: kbr@brugger.dk


use strict;
use warnings;
use Data::Dumper;


# Generic open file that will unzip on the fly if a file is gzip compressed.
# 
# Kim Brugger (03 Aug 2011)
sub open {
  my ($filename) = @_;

  my $fh;

  if ( $filename =~ /gz/) {
    CORE::open ( $fh, "gunzip -c $filename | ") || die "Could not open '$filename': $!\n";
  }
  else {
    CORE::open ( $fh, "$filename") || die "Could not open '$filename': $!\n";
  }

  return $fh;
}




# 
# 
# 
# Kim Brugger (16 Jan 2015)
sub next_line {
  my ($fh) = @_;

  return <$h>;
}



# 
# 
# 
# Kim Brugger (03 Aug 2011)
sub next_fq_entry {
  my ($fh) = @_;

  my $name = <$fh>;
  my $seq = <$fh>;  
  my $strand  =  <$fh>;
  my $qual = <$fh>;

  return 0  if (! $seq);

  return ($name, $seq, $strand, $qual);
  
}




1;



