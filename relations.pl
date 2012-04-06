use Getopt::Std;
use Data::Dumper;


# parse cmd opts
my %opts;
getopts(':1:2:o:', \%opts);

$opts{o}|= "out";


if (!defined $opts{1} || !defined $opts{2}){
    print "Invalid usage\n";
    HELP();
    exit 1;
}


my @rel1, @rel2;
my @vars1, @vars2;
FORM_MATRIX(\@rel1, \@vars1 ,$opts{1});
FORM_MATRIX(\@rel2, \@vars2 ,$opts{2});

(my $join, my $joinVars) = JOIN(\@rel1, \@rel2, \@vars1, \@vars2, "z");
PRINT_OUT($join, $joinVars);

sub FORM_MATRIX {
    (my $relation, my $vars, my $filename) = @_;
    #read from file
    open(REL, $filename || die "Could not open file!");
    @rel_data = <REL>;
    my @relationTemp;
    my $line;
    foreach $line (@rel_data)  {
        (my $var, my $values) = split(";", $line);
        my @rel = ($values =~ /(\d)/g);
        push(@relationTemp, \@rel);
        push(@$vars, $var);
    }
    close REL;
    
    #transpose
    TRANSPOSE(\@relationTemp, $relation);
}

sub PARSE_CONDITION {
    (my $condition) = @_;
    $condition =~ s/\s//g;
    (my $var, my $value) = split("=", $condition);
    return ($var, $value);
}


sub CONDITIONAL_JOIN {
    (my $rel1, my $rel2, my $vars1, my $vars2, my $condition) = @_;
    (my $condVar, my $value) = PARSE_CONDITION($condition);
    #select all $condVar = $value from relation
    (my $newRel1) = SELECT($rel1, $vars1, $condition);
    (my $cutNewRel1, my $newVars1) = CUT($newRel1, $vars1, $condVar);
    (my $newRel2) = SELECT($rel2, $vars2, $condition);
    (my $relation, my $vars) = CARTESIAN($cutNewRel1, $newRel2, $newVars1, $vars2);
    return ($relation, $vars); 
}

#condVar is a string like "k"
sub JOIN {
    (my $rel1, my $rel2, my $vars1, my $vars2, my $condVar) = @_;
    my @joinRelation, @joinVars;

    #select all $condVar = 0 from rel1
    (my $relation_0, my $vars_0) = CONDITIONAL_JOIN($rel1, $rel2, $vars1, $vars2, "$condVar=0");

    #select all $condVar = 1 from rel1
    (my $relation_1, my $vars_1) = CONDITIONAL_JOIN($rel1, $rel2, $vars1, $vars2, "$condVar=1");

    @joinRelation = (@$relation_0, @$relation_1);

    return (\@joinRelation, $vars_0);

}

#lookup is a string like "k"
sub FIND_ROW {
    (my $vars, my $lookup) = @_;
    my $i = 0;
    foreach $var (@$vars) {
        if ($var eq $lookup) {
            return $i;
        }
        $i++;
    }
    return -1;
}

# condition is a string like "var=2"
sub SELECT {
    (my $relation, my $vars, my $condition) = @_;
    my @result;
    (my $var, my $val) = PARSE_CONDITION($condition);
    my $varNum = FIND_ROW($vars, $var);
    
    foreach $row (@$relation) {
        if ($row->[$varNum] == $val) {
            push(@result, $row);
        }
    }
    return \@result;
}

# projection: remove $cutVar from the relation
sub CUT {
    (my $relation, my $vars, my $cutVar) = @_;
    my @result;
    my @resVars;
    my $varNum = FIND_ROW($vars, $cutVar);

    foreach $row (@$relation) {
        my $i = 0;
        my @newRow;
        foreach $value (@$row) {
            if ($i != $varNum) {
                push(@newRow, $value);
            }
            $i++;
        }
        push(@result, \@newRow);
    }
    foreach $var (@$vars) {
        if ($var ne $cutVar) {
            push(@resVars, $var);
        }
    }
    print 
    return (\@result, \@resVars);
}

#cartesian product of rel1 and rel2
sub CARTESIAN {
    (my $rel1, my $rel2, my $vars1, my $vars2) = @_;
    my @result;
    my $rows1 = scalar(@{$rel1});
    my $rows2 = scalar(@{$rel2});
    my $vars1Num = scalar(@$vars1);
    my $vars2Num = scalar(@$vars2);
    for (my $i = 0; $i < $rows1; $i++) {
        for (my $j = 0; $j < $rows2; $j++) {
            my @newCol = (@{$rel1->[$i]}, @{$rel2->[$j]});
            push(@result, \@newCol);
        }
    }
    my @resvars = (@$vars1, @$vars2);
    return (\@result, \@resvars);
}

#pretty print to file
sub PRINT_OUT {
    (my $relation, my $vars) = @_;
    open(OUT, ">", $opts{o}) or die "FAILED to open file for writing!";
    
    #transpose relation (each tuple should be one line)
    my @transposed;
    TRANSPOSE($relation, \@transposed);
    
    my $i = 0;
    foreach my $column (@transposed) {
        print OUT "$vars->[$i]; ";
        $i++;
        foreach my $value (@$column) {
            print OUT "$value ";
        }
        print OUT "\n";
    }
    close OUT;
}

sub TRANSPOSE {
    (my $matrix, my $out) = @_;
    my $columns = scalar(@{$matrix->[0]});
    my $rows = scalar(@$matrix);
    for (my $i=0; $i < $rows; $i++){
        for (my $j=0; $j < $columns; $j++) {
            $out->[$j][$i] = $matrix->[$i][$j];
        }
    }
}

#print usage info
sub HELP {
    print <<EOHELP
    JOIN two relations and receive a new one
    -1 - first relation's table
    -2 - second relations's table
    -o - output

    Typical relation table format: 
    x; 0 0 1 1
    y; 0 1 0 1
    z; 1 1 0 1                          
EOHELP
    ;
}

#load relation from file into a Vhash
sub FORM_HASH {
    (my $hashref, my $filename) = @_;

    open(REL, $filename || die "Could not open file!");
    @rel_data = <REL>;
    my $line;
    foreach $line (@rel_data)  {
        (my $var, my $relation) = split(";", $line);
        my @rel = ($relation =~ /(\d)/g);
        $hashref->{$var} = \@rel;
    }
    close REL;
    print Dumper($hashref);
}
