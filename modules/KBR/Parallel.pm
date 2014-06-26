package KBR::Parallel;
# 
# For running things in parallel...
# 
# 
# Kim Brugger (13 Jun 2011), Contact: kbr@brugger.dk

use strict;
use warnings;
use Data::Dumper;

# Gives an error if it is not the master branch + gives access to version information.

use POSIX ':sys_wait_h';


my @fhs;
my @jobs;
my @commands;
my $MAX_NODES = 8;



# 
# 
# 
# Kim Brugger (20 Sep 2011)
sub max_nodes {
  my ($max_nodes) = @_;

  $MAX_NODES = $max_nodes if (defined  $max_nodes );

  return $MAX_NODES;
}


# 
# 
# 
# Kim Brugger (20 Sep 2011)
sub job_push {
  my (@params) = @_;
  
  push @jobs, [@params];

}

# 
# 
# 
# Kim Brugger (20 Sep 2011)
sub command_push {
  my ($command) = @_;
  
  push @commands, $command;

}


# 
# 
# 
# Kim Brugger (24 Nov 2011)
sub dump_jobs {
  use Data::Dumper;
  print Dumper(\@jobs);
  
}




# 
# 
# 
# Kim Brugger (04 Jan 2011)
sub run_serial {

  my $output;

  while( my $param = shift @jobs) {
    my $function = shift @$param;
    &$function( @$param);
  }
}


# 
# 
# 
# Kim Brugger (04 Jan 2011)
sub run_parallel {  
  my ( $sleep_time, $verbose ) = @_;

  $sleep_time ||= 5;
  $verbose    ||= 0;

  $sleep_time = 5;
  
  my $output;

  my @cpids     = ();
  
  my $done = 0; # to track the number of files handled

  my $running_nodes = 0;
  my $total = 0;

  my $total_jobs = @jobs + @commands;
  print "Total: $total_jobs running: $running_nodes; finished: $done (start)\n" if ( $verbose );

  while (1) {
  FREE_NODE:
    if ($running_nodes <  $MAX_NODES && (int(@jobs) || int(@commands))) {

      if ( @jobs >= 1) {
	my $param = shift @jobs;
	my $function = shift @$param;
      
	my $cpid = create_child($total, $function, @$param);
	$running_nodes++;
	$total++;
	push @cpids, $cpid;
      }
      elsif ( @commands >= 1 ) {
	my $command = shift @commands;
      
	my $cpid = create_command_child($command);
	$running_nodes++;
	$total++;
	push @cpids, $cpid;
      }

      print "Total: $total_jobs running: $running_nodes ($MAX_NODES); finished: $done (create jobs)\n" 
	  if ( $verbose );
#      sleep $sleep_time;

    }
    elsif ( $total < $total_jobs ) {

      print "$total < $total_jobs\n";

      # loop through the nodes to see when one becomes available ...
      while ($done < $total) {
	for (my $i = 0; $i <@cpids; $i++) {
	  next if ($cpids[$i] == -10);
	  
	  my $cpid = $cpids[$i];
	  if (!waitpid($$cpid, WNOHANG)) {
#	  print "Waiting for ($$cpid)\n";
	  }
	  elsif ($$cpid != -10) {
	    $done++;
	    $cpids[$i] = -10;
	    $running_nodes--;
	  }
	}

	print "Total: $total_jobs running: $running_nodes; finished: $done ($total) looping)\n" if ( $verbose );
	sleep $sleep_time;
	
	last if ($running_nodes < $MAX_NODES);
      }
      goto FREE_NODE;
    }

    last if ( $total ==  $total_jobs);
    
    print "Total: $total_jobs running: $running_nodes; finished: $done ($total) looping)\n" if ( $verbose );
    last if ($running_nodes == 0 && (! @jobs || ! @commands) );
  }

  print "Looping for cash\n";

  while ($done < $total) {
    my $running_node = 0;
    for (my $i = 0; $i <@cpids; $i++) {
      next if ($cpids[$i] == -10);
    
      my $cpid = $cpids[$i];
      if (!waitpid($$cpid, WNOHANG)) {
	$running_node++;
      }
      elsif ($$cpid != -10) {
	$done++;
	$cpids[$i] = -10;
      }
    }


    print "Total: $total_jobs running: $running_nodes; finished: $done ($total) (final loop)\n" if ( $verbose );
    sleep $sleep_time;

  }

  foreach my $fh (@fhs ) {
    next if (! $fh);
    $output .= join("", <$fh>);
    close $fh;
  }
  
  return $output;
}




sub create_command_child {
  my ($command) = @_;
  my $pid;

#  print "$command \n";

  if ($pid = fork) {
#    print "PID :: $pid\n";
    ;
  } 
  else {
    die "cannot fork: $!" unless defined $pid;

    # if the process crashes, run it again, with a limit of 2 times...
    my $limit = 2;
    while (system($command)){
      last if ($limit-- == 0);
    };
    exit;
  }
  
  return \$pid;
}


sub create_child {
  my ($id, $command, @params) = @_;

#  print "$command @params\n";

  my $pid;
  if ($pid = open($fhs[$id], "-|")) {
    ;
  } 
  else {
    die "cannot fork: $!" unless defined $pid;
    &$command(@params);
    exit;
  }
  
  return \$pid;
}







1;



