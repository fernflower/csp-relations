use Getopt::Std;
use Data::Dumper;
use List::Member;

# parse cmd opts
my %opts;
getopts(':1:2:o:c:p:', \%opts);

$opts{o}||= "out";
$opts{p}||= "join";

if ($opts{p} eq "join") {
    if (!defined $opts{1} || !defined $opts{2} || !defined $opts{c}){
        print "Invalid usage, choose 2 relations for JOIN and a JOIN-condition\n";
        HELP();
        exit 1;
    }
    my @rel1, @rel2;
    my @vars1, @vars2;
    my $join, $joinVars;
    FORM_MATRIX(\@rel1, \@vars1 ,$opts{1});
    FORM_MATRIX(\@rel2, \@vars2 ,$opts{2});
    my $conditions = PARSE_CONDITION($opts{c});
    my @condVars = keys %$conditions;
    my $firstVar = @condVars[0];
    if ($conditions->{$firstVar} == undef) {
        #unconditional join works only for one variable
        ($join, $joinVars) = JOIN(\@rel1, \@rel2, \@vars1, \@vars2, $firstVar);
    }
    else {
        ($join, $joinVars) = CONDITIONAL_JOIN(\@rel1, \@rel2, \@vars1, \@vars2, $conditions);
    }
    PRINT_OUT($join, $joinVars);
    exit 0;
}

#ok
if ($opts{p} eq "cartesian") {
    if (!defined $opts{1} || !defined $opts{2}){
        print "Invalid usage, choose 2 relations for CARTESIAN\n";
        HELP();
        exit 1;
    }
    my @rel1, @rel2, @vars1, @vars2;
    my $cartesian, $cartesianVars;
    FORM_MATRIX(\@rel1, \@vars1 ,$opts{1});
    FORM_MATRIX(\@rel2, \@vars2 ,$opts{2});
    ($cartesian, $cartesianVars) = CARTESIAN(\@rel1, \@rel2, \@vars1, \@vars2);
    PRINT_OUT($cartesian, $cartesianVars);
    exit 0;
}

#ok
if ($opts{p} eq "remove") {
    if (!defined $opts{1} || !defined $opts{c}){
        print "Invalid usage, choose 1 relation for REMOVE and a REMOVE-condition\n";
        HELP();
        exit 1;
    }
    my @rel1, @vars1;
    my $remove, $removeVars;
    FORM_MATRIX(\@rel1, \@vars1 ,$opts{1});
    my $cutVars = PARSE_REMOVE_CONDITION($opts{c});
    ($remove, $removeVars) = REMOVE(\@rel1, \@vars1, $cutVars);
    PRINT_OUT($remove, $removeVars);
    exit 0;
}

#ok
if ($opts{p} eq "project") {
    if (!defined $opts{1} || !defined $opts{c}){
        print "Invalid usage, choose 1 relation for REMOVE and a REMOVE-condition\n";
        HELP();
        exit 1;
    }
    my @rel1, @vars1;
    my $project, $projectVars;
    FORM_MATRIX(\@rel1, \@vars1 ,$opts{1});
    my $keepVars = PARSE_REMOVE_CONDITION($opts{c});
    ($project, $projectVars) = PROJECT(\@rel1, \@vars1, $keepVars);
    PRINT_OUT($project, $projectVars);
    exit 0;
}

#ok
if ($opts{p} eq "select") {
    if (!defined $opts{1} || !defined $opts{c}){
        print "Invalid usage, choose 1 relation for SELECT and a SELECT-condition\n";
        HELP();
        exit 1;
    }
    my @rel1, @vars1;
    my $select, $selectVars;
    FORM_MATRIX(\@rel1, \@vars1 ,$opts{1});
    $select = SELECT(\@rel1, \@vars1, PARSE_CONDITION($opts{c}));
    PRINT_OUT($select, \@vars1);
    exit 0;
}

else {
    print "Invalid usage!\n";
    HELP();
    exit 1;
}

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

sub PARSE_REMOVE_CONDITION {
    (my $condition) = @_;
    $condition =~ s/\s//g;
    my @result = split(",", $condition);
    return \@result;
}

#return hash: keys - vars, values - condition values
sub PARSE_CONDITION {
    (my $condition) = @_;
    my %result;
    $condition =~ s/\s//g;
    my @matches = split(",", $condition);
    foreach my $match(@matches) {
        (my $var, my $value) = split("=", $match);
        $result{$var} = $value;
    }
    return \%result;
}


# condition is a hash{var}=value
sub CONDITIONAL_JOIN {
    (my $rel1, my $rel2, my $vars1, my $vars2, my $condition) = @_;
    #select all $condVar = $value from relation
    (my $newRel1) = SELECT($rel1, $vars1, $condition);

    #remove vars that are present in both relations from relation1
    my @removeVars;
    foreach my $key(keys %$condition){
        if ((member($key, @$vars1) + 1) and (member($key, @$vars2) + 1)) {
            push(@removeVars, $key);
        }
    }
    (my $cutNewRel1, my $newVars1) = REMOVE($newRel1, $vars1, \@removeVars);
    (my $newRel2) = SELECT($rel2, $vars2, $condition);
    (my $relation, my $vars) = CARTESIAN($cutNewRel1, $newRel2, $newVars1, $vars2);
    return ($relation, $vars); 
}

# TO DO: support other domains (now D=[0,1] only is supported)
# condVar is a string like "k"
sub JOIN {
    (my $rel1, my $rel2, my $vars1, my $vars2, my $condVar) = @_;
    my @joinRelation, @joinVars;
    %cond0 = ($condVar => 0);
    %cond1 = ($condVar => 1);

    #select all $condVar = 0 from rel1
    (my $relation_0, my $vars_0) = CONDITIONAL_JOIN($rel1, $rel2, $vars1, $vars2, \%cond0);
    #select all $condVar = 1 from rel1
    (my $relation_1, my $vars_1) = CONDITIONAL_JOIN($rel1, $rel2, $vars1, $vars2, \%cond1);

    @joinRelation = (@$relation_0, @$relation_1);

    return (\@joinRelation, $vars_0);

}

sub FIND_ROWS {
    (my $vars, my $lookupArr) = @_;
    my @result;
    foreach $var (@$lookupArr) {
        my $num = member($var, @$vars);
        push(@result, $num);
    }
    return \@result;
}

# condition is a hash{var}=value
sub SELECT {
    (my $relation, my $vars, my $condition) = @_;
    my @result;
    #build varNums hash, if any variable doesn't exist -> return null
    my %varNums;
    foreach my $var(keys %$condition) {
        $res = member($var, @$vars);
        if ($res == -1)
        {
            return [];
        }
        $varNums{$var} = $res;
    }

    foreach $row (@$relation) {
        my $addRow = 1;
        foreach my $var(keys %varNums) {
            last if ($addRow == 0);
            if (!($row->[$varNums{$var}] == $condition->{$var})) {
                $addRow = 0; 
            }
        }
        if ($addRow) {
            push(@result, $row);
        }
    }
    return \@result;
}

# projects the relation over projectVars
sub PROJECT {
    (my $relation, my $vars, my $projectVars) = @_;
    my @result;
    my @resVars;
    my $varNums = FIND_ROWS($vars, $projectVars);

    foreach $row (@$relation) {
        my $i = 0;
        my @newRow;
        foreach $value (@$row) {
            if ( member($i, @$varNums) + 1 ) {
                push(@newRow, $value);
            }
            $i++;
        }
        push(@result, \@newRow);
    }
    return (\@result, $projectVars);
}

# like projection, but removes cutVars from the relation
sub REMOVE {
    (my $relation, my $vars, my $cutVars) = @_;
    my @result;
    my @resVars;
    my $varNums = FIND_ROWS($vars, $cutVars);

    foreach $row (@$relation) {
        my $i = 0;
        my @newRow;
        foreach $value (@$row) {
            if ( not (member($i, @$varNums) + 1) ) {
                push(@newRow, $value);
            }
            $i++;
        }
        push(@result, \@newRow);
    }
    foreach $var (@$vars) {
        if ( not (member($var, @$cutVars) + 1) ) {
            push(@resVars, $var);
        }
    }
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
    -p - perform operation: "join", "project", "cartesian", "select" ("join" by default)
    -c - condition ("x", "x=1")

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
