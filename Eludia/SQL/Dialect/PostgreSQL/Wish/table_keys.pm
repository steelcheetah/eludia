#############################################################################

sub wish_to_clarify_demands_for_table_keys {

	my ($i, $options) = @_;

	$i -> {global_name} = 'ix_' . $options -> {table} . '_' . $i -> {name};

	unless (ref $i -> {parts} eq ARRAY) {

		if ($i -> {parts} =~ /\!$/) {

			chop $i -> {parts};

			$i -> {part_uniq} = 1;

		}

		$i -> {parts} = [split /\,\s*/, $i -> {parts}];
	}


	my $MAX_IDENTIFIER_LENGTH_PSQL = 63;
	if (length $i -> {global_name} > $MAX_IDENTIFIER_LENGTH_PSQL) {
		warn "INDEX NAME LENGTH IS GREATER THAN POSTGRESQL LIMIT $MAX_IDENTIFIER_LENGTH_PSQL: '$$i{global_name}'"
			. ". PLEASE REDUCE INDEX NAME LENGTH";
		$i -> {global_name} = substr ($i -> {global_name}, 0, $MAX_IDENTIFIER_LENGTH_PSQL);
	}

	foreach my $part (@{$i -> {parts}}) {

		$part = lc $part;

		$part =~ s{\s}{}gsm;

		$part =~ s{(\w+)\((\d+)\)}{substring($1 from 1 for $2)};

	}

}

################################################################################

sub wish_to_explore_existing_table_keys {

	my ($options) = @_;

	my $existing = {};

	my $len = 4 + length $options -> {table};

	sql_select_loop ("SELECT * FROM pg_indexes WHERE schemaname = current_schema () AND tablename = ? AND indexname NOT LIKE '%_pkey'", sub {

		my $def;

		if ($i -> {indexdef} =~ /\(\s*(.*?)\s*\)/) {

			$def = $1;

		} else {

			darn $i and die "Can't parse index definition (see above)\n";

		}

		my $global_name = lc $i -> {indexname};

		my $d = {

			parts       => [split /\,\s*/, lc $def],

			global_name => $global_name,

			name        => substr $global_name, $len

		};

		$d -> {part_uniq} = 1 if $i -> {indexdef} =~ /UNIQUE(.+)WHERE/i;

		$existing -> {$global_name} = $d;

	}, $options -> {table});

	return $existing;

}

#############################################################################

sub wish_to_actually_create_table_keys {	

	my ($items, $options) = @_;
	
	my $concurrently = $self -> {db} -> {AutoCommit} ? 'CONCURRENTLY' : '';

	foreach my $i (@$items) {

		my ($unique, $where) = $i -> {part_uniq} ? ('UNIQUE', 'WHERE fake = 0') : ('', '');

		sql_do ("CREATE $concurrently $unique INDEX $i->{global_name} ON $options->{table} (@{[ join ', ', @{$i -> {parts}} ]}) $where");
	}

	
}

1;