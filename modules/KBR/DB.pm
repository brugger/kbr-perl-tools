package KBR::DB;
# 
# General connect to databases module, as I seem to recreate this code constantly
# 
# 
# Kim Brugger (13 Jun 2011), contact: kbr@brugger.dk

use strict;
use warnings;
use Data::Dumper;
use Time::HiRes;
use POSIX qw( strftime );

use DBI;


# for caching table column names and queries
my %table_columns;
my %sth_hash;


# 
# 
# 
# Kim Brugger (16 Feb 2012)
sub create_db {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  my $drh = DBI->install_driver("mysql");
  my $rc = $drh->func('createdb', $dbname, $dbhost, $db_user, $db_pass, 'admin');
}


# 
# 
# 
# Kim Brugger (16 Feb 2012)
sub drop_db {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  my $drh = DBI->install_driver("mysql");
  my $rc = $drh->func('dropdb', $dbname, $dbhost, $db_user, $db_pass, 'admin');
}

# 
# 
# 
# Kim Brugger (16 Feb 2012)
sub sql_file {
  my ($dbi, $infile) = @_;

  open( my $in, $infile) || die "Could not open '$infile': $!\n";
  my @statements = split(";", join("", <$in>));
  close( $in );
  
  foreach my $statement ( @statements ) {
    $statement =~ s/\s+//;
    next if ( ! $statement );
    $dbi->do( "$statement;" );
  }
}



# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub connect {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  $dbhost  ||= "localhost";
  $db_user ||= 'kbr_ro';

  my $dbi = DBI->connect("DBI:mysql:$dbname:$dbhost", $db_user, $db_pass) || die "Could not connect to database: $DBI::errstr";

  return $dbi;
}



# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub prepare {
  my ($dbi, $sql) = @_;

  return $sth_hash{$sql} if ( $sth_hash{$sql} );

  my $sth = $dbi->prepare( $sql ) || die "Could not prepare '$sql':$DBI::errstr\n";
  $sth_hash{$sql} = $sth;

  return $sth;
}


# 
# 
# 
# Kim Brugger (07 Mar 2012)
sub do {
  my ($dbi, $sql, @params) = @_;

  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );
  
  $sth->execute( @params ) || die "$DBI::errstr\n";
  $sth = undef; # somethimes this is retained, I dont know why. Reset it just to be safe
  return 1;
}


# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub fetch_array_hash {
  my ($dbi, $sql, @params) = @_;

  
  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );
  
  my @results;

  $sth->execute( @params );
 
  while (my $result = $sth->fetchrow_hashref() ) {
    push @results, $result;
  }

  $sth = undef;  # somethimes this is retained, I dont know why. Reset it just to be safe
  return @results if ( wantarray );
  return \@results;
}


# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub fetch_array_array {
  my ($dbi, $sql, @params) = @_;

  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );
  
  my @results;

  $sth->execute( @params );

  while (my @result_array = $sth->fetchrow_array() ) {
    push @results, \@result_array;
  }

  $sth = undef;  # somethimes this is retained, I dont know why. Reset it just to be safe
  return @results if ( wantarray );
  return \@results;
}


# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub fetch_array {
  my ($dbi, $sql, @params) = @_;

  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );

  $sth->execute( @params );
  
  my @results = $sth->fetchrow_array();

  $sth = undef;  # somethimes this is retained, I dont know why. Reset it just to be safe
  return @results if ( wantarray );
  return \@results;
}

# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub fetch_hash {
  my ($dbi, $sql, @params) = @_;

  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );
  
  $sth->execute( @params );

  my $result = $sth->fetchrow_hashref();

  $sth = undef;  # somethimes this is retained, I dont know why. Reset it just to be safe
  return %$result if ( defined $result && wantarray );
  return $result;
}




# 
# 
# 
# Kim Brugger (18 Feb 2012)
sub  get_column_names {
  my ( $dbi, $table ) = @_;

  return @{$table_columns{$table}} if ( $table_columns{$table});

  my $sth = prepare($dbi, "select * from $table where 1=0");
  my $res = $sth->execute(  );
  my @names = @{$sth->{NAME}};
  $table_columns{$table} = \@names;

  return @{$table_columns{$table}};
}


# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub insert {
  my ($dbi, $table, $hash_refs)  = @_;

  my %columns;
  map { $columns{$_} = 1 } get_column_names( $dbi, $table);

  if ( ref( $hash_refs ) eq "HASH") {
    $hash_refs = [$hash_refs];
    
  }
 
  my (@keys, @all_params, @all_values);
  my $first_insert = 1;
  
  foreach my $hash_ref ( @{ $hash_refs } ) {
  
    if ( !$first_insert && int(@keys) != int( keys %{$hash_ref })) {
      print "Nr of keys in the entries should be idential in each hash\n";
      return 0;
    }

    my @values;
    my @params;

    foreach my $key (keys %$hash_ref ) {
      
      if ( ! $columns{$key}) {
	print STDERR "Column name '$key' is not present in the '$table' table\n";
	return undef;
      }
      if ( $$hash_ref{ $key } ) {
	push @keys, "$key" if ( $first_insert);
	push @params, "?";
	push @values, "$$hash_ref{ $key }";
      }
    }
#    push @all_values, "(". join(",", @values) .")";
    $first_insert = 0 if ( $first_insert );
    push @all_values,  @values;
    push @all_params, "(". join(",", @params) .")";
  }

#  my $query = "INSERT INTO $table (" .join(",", @keys) .") VALUES (".join(",", @params).")";
  my $query = "INSERT INTO $table (" .join(",", @keys) .") VALUES ".join(",", @all_params)."";
#  print "$query\n";
  my $sth = prepare($dbi, $query);
  
  my $execute_value = $sth->execute(@all_values) || die $DBI::errstr;

  return $sth->{mysql_insertid} ||  -100;
}



sub update {
  my ($dbi, $table, $hash_ref, @condition_keys)  = @_;

  my %condition_keys;
  my @conditions;
  map { $condition_keys{ $_ } = 1;
  push @conditions, "$_='$$hash_ref{$_}'";} @condition_keys;

  my %columns;
  map { $columns{$_} = 1 } get_column_names( $dbi, $table);

  my $s = "UPDATE $table SET ";

  my @parts;
  # Build the rest of the sql here ...
  foreach my $key (keys %{$hash_ref}) {
    if ( ! $columns{$key}) {
      print "Column name '$key' is not present in the '$table' table\n";
      return undef;
    }
    # one should not meddle with the id's since it ruins the system
    next if ( $condition_keys{ $key });
    push @parts, "$key = '$$hash_ref{$key}'";
  }

  # collect and make sure we update the right table.
  $s .= join (', ', @parts) ." WHERE " . join(" AND ", @conditions);

  my $sth = $dbi->prepare($s);
  $sth->execute  || die $DBI::errstr;;

  return 1;
}



1;

