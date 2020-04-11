no strict;
no warnings;

################################################################################

sub sql_version {

	my $version = $SQL_VERSION;

	$version -> {strings} = [ sql_select_col ('SELECT version()') ];

	$version -> {string} = $version -> {strings} -> [0];

	($version -> {number}) = $version -> {string} =~ /([\d\.]+)/;

	my @t = split /\./, $version -> {number};

	$version -> {number_tokens} = \@t;

	$version -> {n} = 0 + (join '.', grep {$_} @t [0 .. 1]);

	$version -> {features} -> {'idx.partial'} = ($version -> {n} > 7.1);

	if ($model_update) {

		$model_update -> {schema} = sql_select_scalar ('SELECT current_schema()');

	}

	return $version;

}

################################################################################

sub sql_engine_status {

	return '';
}

################################################################################

sub sql_do_refresh_sessions {

	my $timeout = sql_sessions_timeout_in_minutes ();

	my $ids = sql_select_ids ("SELECT id FROM $conf->{systables}->{sessions} WHERE ts < now() - interval '$timeout minutes'");

	if ($ids ne '-1') {

		sql_do ("DELETE FROM $conf->{systables}->{sessions} WHERE id IN ($ids)");

	}

	sql_do ("UPDATE $conf->{systables}->{sessions} SET ts = now() WHERE id = ?", $_REQUEST {sid}) if $_REQUEST {sid};

}

################################################################################

sub sql_prepare {

	my ($sql, @params) = @_;

	$sql =~ s{^\s+}{};

	if ($preconf -> {core_sql_parse_debug}) {
		warn "original query:\n$sql";
	}

	if ($sql =~ /\bIF\s*\((.+?),(.+?),(.+?)\s*\)/igsm) {

		$sql = mysql_to_postgresql ($sql) if $conf -> {core_auto_postgresql};

		($sql, @params) = sql_extract_params ($sql, @params) if ($conf -> {core_sql_extract_params} && $sql =~ /^\s*(SELECT|INSERT|UPDATE|DELETE)/i);

	} else {

		($sql, @params) = sql_extract_params ($sql, @params) if ($conf -> {core_sql_extract_params} && $sql =~ /^\s*(SELECT|INSERT|UPDATE|DELETE)/i);

		$sql = mysql_to_postgresql ($sql) if $conf -> {core_auto_postgresql};

	}

	if ($preconf -> {core_sql_parse_debug}) {
		warn "postgresql query:\n$sql";
	}

	my $location = "-- type='$_REQUEST{type}', id='$_REQUEST{id}', action='$_REQUEST{action}', user=$_USER->{id}, process=$$";

	if ($_REQUEST {__lrt_fork}) {
		$location .= ", __lrt_fork=1";
	}

	if ($preconf -> {core_sql_location}) {
		$location .= "\n-- location=" . sql_location ();
	}

	$sql = "$location\n" . $sql;

	my $st;

	if ($preconf -> {db_cache_statements}) {
		eval {$st = $db  -> prepare_cached ($sql, {}, 3)};
	}
	else {
		eval {$st = $db  -> prepare ($sql, {})};
	}

	if ($@) {
		my $msg = "sql_prepare: $@ (SQL = $sql)\n";
		print STDERR $msg;
		die $msg;
	}

	return ($st, @params);

}

################################################################################

sub sql_location {

__profile_in ('sql.sql_location');

	my ($options) = @_;

	$options -> {max_depth}   ||= 30;

	my $stack_frame = $options -> {start_frame};

	@call_details = caller ($stack_frame);

	my @stack;

	while (@call_details && $stack_frame < $options -> {max_depth}) {

		my ($subroutine, $line, $module) = reverse splice (@call_details, 1, 3);

		$subroutine =~ s/\w+:://;

		if ($module !~ /\beludia\b/i) {
			__profile_out ('sql.sql_location');
			return "${module}:${line}:$subroutine";
		}

		@call_details = caller (++$stack_frame);
	}

__profile_out ('sql.sql_location');
	return '';
}

################################################################################

sub sql_do {

	darn \@_ if $preconf -> {core_debug_sql_do};

	my ($sql, @params) = @_;

	my ($st, @params) = sql_prepare ($sql, @params);

	sql_safe_execute ($st, \@params);

	$st -> finish;

}

################################################################################
#
#sub sql_execute_procedure {

#	my ($sql, @params) = @_;

#	my $time = time;

#	$sql .= ';' unless $sql =~ /;[\n\r\s]*$/;

#	(my $st, @params) = sql_prepare ($sql, @params);

#	my $i = 1;
#	while (@params > 0) {
#		my $val = shift (@params);
#		if (ref $val eq 'SCALAR') {
#			$st -> bind_param_inout ($i, $val, shift (@params));
#		} else {
#			$st -> bind_param ($i, $val);
#		}
#		$i ++;
#	}

#	eval {
#		$st -> execute;
#	};


#	if ($@) {
#		local $SIG {__DIE__} = 'DEFAULT';
#		if ($@ =~ /ORA-\d+:(.*)/) {
#			die "$1\n";
#	  } else {
#			die $@;
#		}
#
#	}

#	$st -> finish;

#}

################################################################################

sub sql_select_all_cnt {

	my ($sql, @params) = @_;

	if ($preconf -> {use_old_select_all_cnt}) {
		return (_sql_select_all_cnt_old ($sql, @params));
	}

	$sql =~ s{LIMIT\s+(\d+)\s*\,\s*(\d+).*}{LIMIT $2 OFFSET $1}ism;

	my $options = {};

	if (@params > 0 and ref ($params [-1]) eq HASH) {
		$options = pop @params;
	}

	$sql = sql_adjust_fake_filter ($sql, $options);

	if ($_REQUEST {xls} && $conf -> {core_unlimit_xls} && !$_REQUEST {__limit_xls}) {
		$sql =~ s{\bLIMIT\b.*}{}ism;
		my $result = sql_select_all ($sql, @params, $options);
		my $cnt = ref $result eq ARRAY ? 0 + @$result : -1;
		return ($result, $cnt);
	}

	$sql =~ m/\brows_cnt\b/
		and die "'rows_cnt' is reserved from sql_select_all_cnt";

	my ($processed_sql, $is_placed_cnt);
	while ($sql =~ m/\bFROM\b|\(/ism) {
		if ($& eq 'FROM') {
			$sql = $processed_sql . $` . ", COUNT (*) OVER() AS rows_cnt FROM" . $';
			$is_placed_cnt = 1;
			last;
		} elsif ($& eq '(') {
			my $quotes_cnt = 1;
			$processed_sql .= $` . $&;
			while ($' =~ m/\)|\(/ism) {
				if ($& eq ')') {
					$quotes_cnt --;
				} else {
					$quotes_cnt ++;
				}
				$processed_sql .= $` . $&;
				$sql = $';
				last if !$quotes_cnt;
			}
		}
	}

	$is_placed_cnt
		or die "Unable to get COUNT(*)";

	my ($st, @params) = sql_prepare ($sql, @params);

	sql_safe_execute ($st, \@params);

	my @result = ();
	my $cnt = 0;

	while (my $i = $st -> fetchrow_hashref ()) {
		push @result, lc_hashref ($i);
		$cnt = delete $i -> {rows_cnt};
	}

	$st -> finish;

	return (\@result, $cnt);

}

################################################################################

sub _sql_select_all_cnt_old {

	my ($sql, @params) = @_;

	$sql =~ s{LIMIT\s+(\d+)\s*\,\s*(\d+).*}{LIMIT $2 OFFSET $1}ism;

	my $options = {};

	if (@params > 0 and ref ($params [-1]) eq HASH) {
		$options = pop @params;
	}

	$sql = sql_adjust_fake_filter ($sql, $options);

	if ($_REQUEST {xls} && $conf -> {core_unlimit_xls} && !$_REQUEST {__limit_xls}) {
		$sql =~ s{\bLIMIT\b.*}{}ism;
		my $result = sql_select_all ($sql, @params, $options);
		my $cnt = ref $result eq ARRAY ? 0 + @$result : -1;
		return ($result, $cnt);
	}

	my ($st, @params) = sql_prepare ($sql, @params);

	sql_safe_execute ($st, \@params);

	my @result = ();

	while (my $i = $st -> fetchrow_hashref ()) {
		push @result, lc_hashref ($i);
	}

	$st -> finish;

	$sql =~ s{ORDER BY.*}{}ism;

	my @cnt_select = $sql =~ /\bSELECT\b/ig;

	if (@cnt_select > 1) { # find main query FROM by same indent as SELECT
		my $indent = $sql =~ m/^(\s*)SELECT/i? $1 : '';
		unless ($sql =~ s/SELECT.*\n${indent}FROM\b/SELECT COUNT(*) OVER() FROM/ism) {
			die "sql_select_all_cnt: Unable to get COUNT(*)";
		}
	} else {
		$sql =~ s/SELECT.*?[\n\s]+FROM[\n\s]+/SELECT COUNT(*) OVER() FROM /ism;
	}

	my $cnt = sql_select_scalar ($sql, @params);

	return (\@result, $cnt);

}

################################################################################

sub sql_select_all {

	my ($sql, @params) = @_;

	$sql =~ s{LIMIT\s+(\d+)\s*\,\s*(\d+).*}{LIMIT $2 OFFSET $1}ism;

	my $options = {};

	if (@params > 0 and ref ($params [-1]) eq HASH) {
		$options = pop @params;
	}

	$sql = sql_adjust_fake_filter ($sql, $options);

	my ($st, @params) = sql_prepare ($sql, @params);

	sql_safe_execute ($st, \@params);

	return $st if $options -> {no_buffering};

	my @result = ();

	while (my $i = $st -> fetchrow_hashref ()) {
		push @result, lc_hashref ($i);
	}

	$st -> finish;

	$_REQUEST {__benchmarks_selected} += @result;

	return \@result;

}

################################################################################

sub sql_select_all_hash {

	my ($sql, @params) = @_;

	my $options = {};

	if (@params > 0 and ref ($params [-1]) eq HASH) {
		$options = pop @params;
	}

	$sql = sql_adjust_fake_filter ($sql, $options);

	my $result = {};

	my ($st, @params) = sql_prepare ($sql, @params);

	sql_safe_execute ($st, \@params);

	while (my $r = $st -> fetchrow_hashref) {
		lc_hashref ($r);
		$result -> {$r -> {id}} = $r;
	}

	$st -> finish;

	return $result;

}

################################################################################

sub sql_select_col {

	my ($sql, @params) = @_;

	my @result = ();

	$sql =~ s{LIMIT\s+(\d+)\s*\,\s*(\d+).*}{LIMIT $2 OFFSET $1}ism;

	my ($st, @params) = sql_prepare ($sql, @params);

	sql_safe_execute ($st, \@params);

	while (my @r = $st -> fetchrow_array ()) {
		push @result, @r;
	}

	$st -> finish;

	return @result;

}

################################################################################

sub lc_hashref {

	my ($hr) = @_;

	return $hr;

}

################################################################################

sub sql_select_hash {

	my ($sql_or_table_name, @params) = @_;

	$sql =~ s{LIMIT\s+(\d+)\s*\,\s*(\d+).*}{LIMIT $2 OFFSET $1}ism;

	if ($sql_or_table_name !~ /^\s*(SELECT|WITH)\b/i) {

		my $id = $_REQUEST {id};

		my $field = 'id';

		if (@params) {
			if (ref $params [0] eq HASH) {
				($field, $id) = each %{$params [0]};
			} else {
				$id = $params [0];
			}
		}

		@params = ({}) if (@params == 0);

		$_REQUEST {__the_table} ||= $sql_or_table_name;

		return sql_select_hash ("SELECT * FROM $sql_or_table_name WHERE $field = ?", $id);

	}

	if (!$_REQUEST {__the_table} && $sql_or_table_name =~ /\s+FROM\s+(\w+)/sm) {

		$_REQUEST {__the_table} = $1;

	}

	my ($st, @params) = sql_prepare ($sql_or_table_name, @params);

	sql_safe_execute ($st, \@params);

	my $result = $st -> fetchrow_hashref ();

	$st -> finish;

	return lc_hashref ($result);

}

################################################################################

sub sql_select_array {

	my ($sql, @params) = @_;

	$sql =~ s{LIMIT\s+(\d+)\s*\,\s*(\d+).*}{LIMIT $2 OFFSET $1}ism;

	my ($st, @params) = sql_prepare ($sql, @params);

	sql_safe_execute ($st, \@params);

	my @result = $st -> fetchrow_array ();

	$st -> finish;

	return wantarray ? @result : $result [0];

}

################################################################################

sub sql_select_scalar {

	my ($sql, @params) = @_;

	my @result;

	$sql =~ s{LIMIT\s+(\d+)\s*\,\s*(\d+).*}{LIMIT $2 OFFSET $1}ism;

	my ($st, @params) = sql_prepare ($sql, @params);

	sql_safe_execute ($st, \@params);

	@result = $st -> fetchrow_array ();

	$st -> finish;

	return $result [0];

}

################################################################################

sub sql_select_path {

	my ($table_name, $id, $options) = @_;

	$options -> {name} ||= 'name';
	$options -> {type} ||= $table_name;
	$options -> {id_param} ||= 'id';

	my ($parent) = $id;

	my @path = ();

	while ($parent) {
		my $r = sql_select_hash ("SELECT id, parent, $$options{name} as name, '$$options{type}' as type, '$$options{id_param}' as id_param FROM $table_name WHERE id = ?", $parent);
		$r -> {cgi_tail} = $options -> {cgi_tail},
		unshift @path, $r;
		$parent = $r -> {parent};
	}

	if ($options -> {root}) {
		unshift @path, {
			id => 0,
			parent => 0,
			name => $options -> {root},
			type => $options -> {type},
			id_param => $options -> {id_param},
			cgi_tail => $options -> {cgi_tail},
		};
	}

	return \@path;

}

################################################################################

sub sql_select_subtree {

	my ($table_name, $id, $options) = @_;

	$options -> {filter} = " AND $options->{filter}"
		if $options->{filter};
	my @ids = ($id);

	while (TRUE) {

		my $ids = join ',', @ids;

		my @new_ids = sql_select_col ("SELECT id FROM $table_name WHERE fake = 0 AND parent IN ($ids) AND id NOT IN ($ids) $options->{filter}");

		last unless @new_ids;

		push @ids, @new_ids;

	}

	return @ids;

}

################################################################################

sub sql_last_insert_id {
	return $__last_insert_id || sql_select_scalar ("SELECT lastval()") || 0;
}

################################################################################

sub sql_set_sequence {

	my ($seq_name, $value) = @_;
	return sql_select_scalar ("SELECT setval('$seq_name', $value)");

}

################################################################################

sub sql_do_insert {

	my ($table_name, $data) = @_;

	delete_fakes ($table_name);

	exists $data -> {fake} or $data -> {fake} = $_REQUEST {sid};

	if (is_recyclable ($table_name)) {

		assert_fake_key ($table_name);

		### all orphan records are now mine

		sql_do (<<EOS, $_REQUEST {sid});
			UPDATE
				$table_name
			SET
				fake = ?
			WHERE
				$table_name.fake > 0
			AND
				$table_name.fake NOT IN (SELECT id FROM $conf->{systables}->{sessions})
EOS

		### get my least fake id (maybe ex-orphan, maybe not)

		$__last_insert_id = sql_select_scalar ("SELECT id FROM $table_name WHERE fake = ? ORDER BY id LIMIT 1", $_REQUEST {sid});

		if ($__last_insert_id) {
			sql_do ("DELETE FROM $table_name WHERE id = ?", $__last_insert_id);
			$data -> {id} = $__last_insert_id;
		}

	}

	my ($fields, $args, @params) = ('', '');

	my $table = $DB_MODEL -> {tables} -> {$table_name};

	$data -> {id} or delete $data -> {id};

	while (my ($k, $v) = each %$data) {

		if (exists $table -> {columns} -> {$k} -> {NULLABLE}
			&& $table -> {columns} -> {$k} -> {NULLABLE} == 0
			&& exists $table -> {columns} -> {$k} -> {COLUMN_DEF}
			&& !defined $v
		) {

			$v = $table -> {columns} -> {$k} -> {COLUMN_DEF};

		}

		defined $v or next;

		my $comma = @params ? ', ' : '';
		$fields .= "$comma $k";
		$args   .= "$comma ?";

		$v    = $v eq '' ? undef : $v + 0
			if $table -> {columns} -> {$k} -> {TYPE_NAME} =~ /.*(int|decimal).*/;
		$v    = $v eq '' || $v lt '0001-01-01' ? undef : $v
			if $table -> {columns} -> {$k} -> {TYPE_NAME} =~ /.*date.*/;

		push @params, $v;

	}

	my $sql = "INSERT INTO $table_name ($fields) VALUES ($args)";

	if ($data -> {id}) {

		sql_do ($sql, @params);

		sql_check_seq ($table_name);

	}
	else {

		$data -> {id} = sql_select_scalar ("$sql RETURNING id", @params);

	}

	return $data -> {id};

}

################################################################################

sub sql_check_seq {

	my ($table) = @_;

	my $max = sql_select_scalar ("SELECT MAX(id) FROM $table");

	sql_select_scalar ("SELECT setval('${table}_id_seq', ?)", $max) if $max > 0;

}

################################################################################

sub sql_do_delete {

	my ($table_name, $options) = @_;

	if (ref $options -> {file_path_columns} eq ARRAY) {

		map {sql_delete_file ({table => $table_name, path_column => $_})} @{$options -> {file_path_columns}}

	}

	our %_OLD_REQUEST = %_REQUEST;

	eval {
		my $item = sql_select_hash ($table_name);
		foreach my $key (keys %$item) {
			$_OLD_REQUEST {'_' . $key} = $item -> {$key};
		}
	};

	sql_do ("DELETE FROM $table_name WHERE id = ?", $_REQUEST{id});

	delete $_REQUEST{id};

}

################################################################################

sub sql_delete_file {

	my ($options) = @_;

	if ($options -> {path_column}) {
		$options -> {file_path_columns} = [$options -> {path_column}];
	}

	$options -> {id} ||= $_REQUEST {id};

	foreach my $column (@{$options -> {file_path_columns}}) {
		my $path = sql_select_array ("SELECT $column FROM $$options{table} WHERE id = ?", $options -> {id});
		delete_file ($path);
	}


}

################################################################################

sub sql_download_file {

	my ($options) = @_;

	$_REQUEST {id} ||= $_PAGE -> {id};

	my $item = sql_select_hash ("SELECT * FROM $$options{table} WHERE id = ?", $_REQUEST {id});
	$options -> {size} = $item -> {$options -> {size_column}};
	$options -> {path} = $item -> {$options -> {path_column}};
	$options -> {type} = $item -> {$options -> {type_column}};
	$options -> {file_name} = $item -> {$options -> {file_name_column}};

#	if ($options -> {body_column}) {

#		my $time = time;

#		my $sql = "SELECT $options->{body_column} FROM $options->{table} WHERE id = ?";
#		my $st = $db -> prepare ($sql, {ora_auto_lob => 0});
#		$st -> execute ($_REQUEST {id});
#		(my $lob_locator) = $st -> fetchrow_array ();

#		my $chunk_size = 1034;
#		my $offset = 1 + download_file_header (@_);

#		while (my $data = $db -> ora_lob_read ($lob_locator, $offset, $chunk_size)) {
#		      $r -> print ($data);
#		      $offset += $chunk_size;
#		}

#		$st -> finish ();

#	}
#	else {
		download_file ($options);
#	}

}

################################################################################

#sub sql_store_file {

#	my ($options) = @_;

#	my $st = $db -> prepare ("SELECT $options->{body_column} FROM $options->{table} WHERE id = ? FOR UPDATE", {ora_auto_lob => 0});

#	$st -> execute ($options -> {id});
#	(my $lob_locator) = $st -> fetchrow_array ();
#	$st -> finish ();

#	$db -> ora_lob_trim ($lob_locator, 0);

#	$options -> {chunk_size} ||= 4096;
#	my $buffer = '';

#	open F, $options -> {real_path} or die "Can't open $options->{real_path}: $!\n";

#	while (read (F, $buffer, $options -> {chunk_size})) {
#		$db -> ora_lob_append ($lob_locator, $buffer);
#	}

#	sql_do (
#		"UPDATE $$options{table} SET $options->{size_column} = ?, $options->{type_column} = ?, $options->{file_name_column} = ? WHERE id = ?",
#		-s $options -> {real_path},
#		$options -> {type},
#		$options -> {file_name},
#		$options -> {id},
#	);

#	close F;

#}

################################################################################

sub sql_upload_file {

	my ($options) = @_;

	$options -> {id} ||= $_REQUEST {id};

	my $uploaded = $options -> {upload} || upload_file ($options) or return;

	$options -> {body_column} or sql_delete_file ($options);

#	if ($options -> {body_column}) {

#		$options -> {real_path} = $uploaded -> {real_path};

#		sql_store_file ($options);

#		unlink $uploaded -> {real_path};

#		delete $uploaded -> {real_path};

#	}

	my (@fields, @params) = ();

	foreach my $field (qw(file_name size type path)) {
		my $column_name = $options -> {$field . '_column'} or next;
		push @fields, "$column_name = ?";
		push @params, $uploaded -> {$field};
	}

	foreach my $field (keys (%{$options -> {add_columns}})) {
		push @fields, "$field = ?";
		push @params, $options -> {add_columns} -> {$field};
	}

	if (@fields) {

		my $tail = join ', ', @fields;

		sql_do ("UPDATE $$options{table} SET $tail WHERE id = ?", @params, $options -> {id});

	}

	return $uploaded;

}

################################################################################

sub keep_alive {
	my $sid = shift;
	sql_do ("UPDATE $conf->{systables}->{sessions} SET ts = now() WHERE id = ? ", $sid);
}

################################################################################

sub sql_select_loop {

	my ($sql, $coderef, @params) = @_;

	my ($st, @params) = sql_prepare ($sql, @params);

	sql_safe_execute ($st, \@params);

	local $i;

	while ($i = $st -> fetchrow_hashref) {
		lc_hashref ($i);
		&$coderef ();
	}

	$st -> finish ();

}

#################################################################################

sub mysql_to_postgresql {

my ($sql) = @_;

our $mysql_to_postgresql_cache;

my $cached = $mysql_to_postgresql_cache -> {$sql};

my $src_sql = $sql;

return $cached if $cached;

my (@items,@group_by_values_ref,@group_by_fields_ref);
my ($pattern,$need_group_by);
my $sc_in_quotes=0;

#warn "ORACLE IN: <$sql>\n";

############### Заменяем неразрешенные в запросах слова на ключи (обратно восстанавливаем в lc_hashref())
# $sql =~ s/([^\W]\s*\b)user\b(?!\.)/\1RewbfhHHkgkglld/igsm;
#$sql =~ s/([^\W]\s*\b)level\b(?!\.)/\1NbhcQQehgdfjfxf/igsm;
############### Вырезаем и запоминаем все что внутри кавычек, помечая эти места.
if ($sql !~ m/INSERT\s+INTO/igsm && $sql !~ m/^COMMENT ON COLUMN/igsm) {
	$sql =~ s/\"/\'/igsm;
}
$sql =~ s/\`//igsm;

while ($sql =~ /(''|'.*?[^\\]')/ism)
{
	my $temp = $1;
	# Скобки и запятые внутри кавычек прячем чтобы не мешались при анализе и замене функций
	$temp =~ s/\(/JKghsdgfweftyfd/gsm;
	$temp =~ s/\)/RTYfghhfFGhhjJg/gsm;
	$temp =~ s/\,/DFgpoUUYTJjkgJj/gsm;
	$in_quotes[++$sc_in_quotes]=$temp;
	$sql =~ s/''|'.*?[^\\]'/POJJNBhvtgfckjh$sc_in_quotes/ism;
}

### Убираем пробелы перед скобками
$sql =~ s/\s*(\(|\))/\1/igsm;

############### Делаем из выражений в скобках псевдофункции чтобы шаблон свернулся
while ($sql =~ s/([^\w\s]+?\s*)(\()/\1VGtygvVGVYbbhyh\2/ism) {};
############### Это убираем

$sql =~ s/\bBINARY\b//igsm;
#$sql =~ s/\bAS\b\s+(?!\bSELECT\b)//igsm;
# $sql =~ s/(.*?)#.*?\n/\1\n/igsm; 		 				# Убираем закомментированные строки
$sql =~ s/STRAIGHT_JOIN//igsm;
$sql =~ s/FORCE\s+INDEX\(.*?\)//igsm;

############### COUNT(*) OVER()
$sql =~ s/COUNT\(\*\)\s*OVER\(\)/CnTOveR/igsm;

############### Обработка UPDATE...JOIN... (работает только для 1 join, возможно, убрать в дальнейшем)
if ($sql =~ m/\bUPDATE\b(.+)\bJOIN\b(.+)\bON\b(.+)\bSET\b(.+)\bWHERE\b(.+)/igsm) {

	my $set_constr = $4;
	my $full_table = $1;

	$full_table =~ s/\b(LEFT|RIGHT|INNER|FULL)\b//igsm;
	$full_table =~ m/(\w+)\s*(\w+)?/igsm;

	my $target_table = $2 eq "" ? $1 : $2 ;
	$set_constr =~ s/$target_table\.(\w*)\s*=/$1 =/igsm;

	$sql =~ s/\bUPDATE\b(.+)\bJOIN\b(.+)\bON\b(.+)\bSET\b(.+)\bWHERE\b(.+)/UPDATE $full_table SET $set_constr FROM $2 WHERE$5 AND$3/igsm;
}

############### Вырезаем функции начиная с самых вложенных и совсем не вложенных
# места помечаем ключем с номером, а сами функции с аргументами запоминаем в @items
# до тех пор пока всё не вырежем
while ($sql =~m/((\b\w+\((?!.*\().*?)\))/igsm)
{
	$items[++$sc]=$1;
	$sql =~s/((\b\w+\((?!.*\().*?)\))/NJNJNjgyyuypoht$sc/igsm;
}

$pattern = $sql;

if ($sql =~ /SELECT.+LIMIT/ism) {
	$sql =~ s{LIMIT\s+(\d+)\s*\,\s*(\d+).*}{LIMIT $2 OFFSET $1}ism;
}
############### Вставляем AS перед алиасами (на случай совпадения их с ключевыми словами)
if ($sql =~ m/\bSELECT\b.*\bFROM\b/ims) {
  my $select_from = [];
  my $sfi = 0;

  while ($sql =~ m/\bSELECT\b.*\bFROM\b/igms) {
    my $str;
    if ($sql =~ s/(\bSELECT\b)(.*?)(\bSELECT\b.*?\bFROM\b)/$1$2SlCtFrM$sfi/ims) {

      $str = $3;
    } elsif ($sql =~ s/\bSELECT\b.*\bFROM\b/SlCtFrM$sfi/ims) { # замена последнего оставшегося select...from
      $str = $&;
    }

    my $case_end = [];
    my $csi=0;

    if ($str =~ m/\bCASE\b.*\bEND\b/ims) {
      while ($str =~ s/(\bCASE\b)(.*)(\1.*?\bEND\b)/$1$2CSNDcsnd$csi/ims) {
        $case_end[$csi++] = $3;
      }
      $str =~ s/\bCASE\b.*\bEND\b/CSNDcsnd$csi/ims; # замена последнего оставшегося case...end
      $case_end[$csi] = $&;
    }

    if ($str =~ m/\bSELECT\b\s*(DISTINCT\s*)?(.*)\bFROM\b/igms) {
      my $list_col = $2;
      $list_col =~ s/(\w+\b|\?)(\s+AS\s*)?\s+\b(\w+)/$1 AS $3/igms;
      $str =~ s/(\bSELECT\b\s*(DISTINCT\s*)?)(.*)(\bFROM\b)/$1$list_col$4/ims;
    }

    while ($str =~ s/CSNDcsnd(\d+)/$case_end[$1]/igsm) {}
    $select_from[$sfi++] = $str;
  }

  while ($sql =~ s/SlCtFrM(\d+)/$select_from[$1]/igsm) {}
}

############### ORDER BY
$sql =~ s/\s*ORDER\s+BY\s+NULL(\W\s*|$)/ /igsm; ### \W - для обработки случаев типа ORDER BY NULLIF(...)

if ($sql =~ m/(\s*ORDER\s+BY\s*)([^\)]+)/igsm) {
	my $old_order_by = $1 . $2;
	my @order_by = split ',', $2;

	foreach my $field (@order_by) {  ######## Обработка ORDER BY(... + 0)
		$field =~ s/(\b.+\b)\s*\+\s*0/CAST($1 AS INTEGER)/igms or next;
		next if ($field =~ m/\bNULLS\s+(FIR|LA)ST\b/igsm);
		$field .= ($field =~ m/\bDESC\b/igsm) ? ' NULLS LAST ' : ' NULLS FIRST ';
	}

	$new_order_by = join ',', @order_by;
	$old_order_by =~ s/([+.()?])/\\$1/igsm;

	$sql =~ s/$old_order_by/ ORDER BY $new_order_by/igsm;
}
$sql =~ s/\bINTERVAL\s+(\S+)\s+(\w+)/$1\:\:INTERVAL $2/igsm;

#$need_group_by=1 if ( $sql =~ m/\s+GROUP\s+BY\s+/igsm);

#if ($need_group_by) {

	# Запоминаем значения из GROUP BY до UNION или ORDER BY или HAVING
	# Также формируем массив хранящий ссылки на массивы значений для каждого SELECT
#	my $sc=0;
#	while ($sql =~ s/\s+GROUP\s+BY\s+(.*?)(\s+HAVING\s+|\s+UNION\s+|\s+ORDER\s+BY\s+|$)/VJkjn;lohggff\2/ism) {
#		my @group_by_values = split(',',$1);
#		$group_by_values_ref[$sc++]=\@group_by_values;
#	}


#	my $sc=0;
	# Разбиваем шаблон от SELECT до FROM на поля для дальнейшего раздельного наполнения
	# и подстановки в GROUP BY вместо цифр
#	while ($pattern =~ s/\bSELECT(.*?)\bFROM\b//ism) {
#		my @group_by_fields = split (',',$1);
		# Удаляем алиасы
#		for (my $i = 0; $i <= $#group_by_fields; $i++) {
#			$group_by_fields[$i] =~ s/^\s*//igsm;
#			$group_by_fields[$i] =~ s/\s+.*//igsm;
#		}
#		$group_by_fields_ref[$sc++]=\@group_by_fields;
#	}
#}

# Если в шаблоне нет FROM - взводим флаг чтобы после замен добавить FROM DUAL
# Делаем так потому что внутри ORACLE функции EXTRACT есть FROM
#my $need_from_dual=1 if ($sql =~ m/^\s*SELECT\b/igsm && not ($sql =~ m/\bFROM\b/igsm));

# Делаем замену и собираем исходный SQL начиная с нижних уровней
for(my $i = $#items; $i >= 1; $i--) {
	# Восстанавливаем то что было внутри кавычек в аргументах функций
	$items[$i] =~ s/POJJNBhvtgfckjh(\d+)/$in_quotes[$1]/igsm;
	######################### Блок замен SQL синтаксиса #########################
	$items[$i] =~ s/\bIFNULL(\(.*?\))/COALESCE\1/igsm;
	$items[$i] =~ s/\bRAND(\(.*?\))/RANDOM\1/igsm;
	$items[$i] =~ s/\bUUID(\(\))/CAST\(uuid_generate_v4\(\) AS text\)/igsm;
	$items[$i] =~ s/\b(?:OLD_)?PASSWORD(\(.*?\))/MD5\1/igsm;
	$items[$i] =~ s/\bCONCAT\((.*?)\)/join('||',split(',',$1))/iegsm;
	$items[$i] =~ s/\bSUBSTR\((.+?),(.+?),(.+?)\)/SUBSTRING\(\1,\2,\3\)/igsm;
	$items[$i] =~ s/\bLEFT\((.+?),(.+?)\)/SUBSTRING\(\1,1,\2\)/igsm;
	$items[$i] =~ s/\bRIGHT\((.+?),(.+?)\)/SUBSTRING\(\1,LENGTH\(\1\)-\(\2\)+1,LENGTH\(\1\)\)/igsm;
	$items[$i] =~ s/\bFIELD\((.+?), (.+?)\)/ARRAY_POSITION\(ARRAY[\2], \1\)/igsm;
	$items[$i] =~ s/(\bSUBDATE\b|\bDATE_SUB\b)\((.+?),\s*\w*?\s*\?\s*(\w+)\)/$2 - CAST(? || '$3' AS interval)/igsm;
	$items[$i] =~ s/(\bADDDATE\b|\bDATE_ADD\b)\((.+?),\s*\w*?\s*\?\s*(\w+)\)/$2 + CAST(? || '$3' AS interval)/igsm;
	$items[$i] =~ s/(\bSUBDATE\b|\bDATE_SUB\b)\((.+?),\s*\w*?\s*(\d+)\s*(\w+)\)/$2 - interval '$3 $4'/igsm;
	$items[$i] =~ s/(\bADDDATE\b|\bDATE_ADD\b)\((.+?),\s*\w*?\s*(\d+)\s*(\w+)\)/$2 + interval '$3 $4'/igsm;

	$items[$i] =~ s/\bGROUP_CONCAT\((.*?) ORDER BY (.+?)\sSEPARATOR '(.+?)'\)/STRING_AGG($1, '$3' ORDER BY $2)/igsm;
	$items[$i] =~ s/\bGROUP_CONCAT\((.*?)\sSEPARATOR '(.+?)'\)/STRING_AGG($1, '$2')/igsm;
	$items[$i] =~ s/\bGROUP_CONCAT\(DISTINCT (.*?)\)/STRING_AGG(DISTINCT CAST($1 AS text), ',')/igsm;
	$items[$i] =~ s/\bGROUP_CONCAT\((.*?)\)/STRING_AGG(CAST($1 AS text), ',')/igsm;
	$items[$i] =~ s/\bFIND_IN_SET\((.+?), (.+?)\)/CAST($1 AS text) = ANY(string_to_array($2, ','))/igsm;


	$items[$i] =~ s/\bDAYOFMONTH\((.*?)\)/DATE_PART('day', $1)/igsm;
	$items[$i] =~ s/\bdatabase\(\)/current_database()/igsm;
	$items[$i] =~ s/\bTIMEDIFF\((.+?),(.+?)\)/$1 - $2/igsm;
	if ($items[$i] =~ m/\bSTR_TO_DATE\((.+?),(.+?)\)/igsm) {
		my $expression = $1;
		my $format = $2;
		$format =~ s/%Y/YYYY/igsm;
		$format =~ s/%y/YY/igsm;
		$format =~ s/%d/DD/igsm;
		$format =~ s/%m/MM/igsm;
		$format =~ s/%H/HH24/igsm;
		$format =~ s/%h/HH12/igsm;
		$format =~ s/%i/MI/igsm;
		$format =~ s/%s/SS/igsm;
		$format =~ s/%p/AM/igsm;
		$items[$i] = "TO_DATE($expression,$format)";
	}

#	if ($model_update -> {characterset} =~ /UTF/i) {
#		$items[$i] =~ s/\bHEX(\(.*?\))/RAWTONHEX\1/igsm;
#	}
#	else {
#		$items[$i] =~ s/\bHEX(\(.*?\))/RAWTOHEX\1/igsm;
#	}
	####### DATE_FORMAT
	if ($items[$i] =~ m/\bDATE_FORMAT\((.+?),(.+?)\)/igsm) {
		my $expression = $1;
		my $format = $2;
		$format =~ s/%Y/YYYY/igsm;
		$format =~ s/%y/YY/igsm;
		$format =~ s/%d/DD/igsm;
		$format =~ s/%m/MM/igsm;
		$format =~ s/%H/HH24/igsm;
		$format =~ s/%h/HH12/igsm;
		$format =~ s/%i/MI/igsm;
		$format =~ s/%s/SS/igsm;
		$format =~ s/%p/AM/igsm;
		$items[$i] = "TO_CHAR ($expression,$format)";
	}

	######## CURDATE()
	$items[$i] =~ s/\bCURDATE\(.*?\)/CURRENT_DATE/igsm;
	$items[$i] =~ s/\bCURRENT_DATE\(\)/CURRENT_DATE/igsm;
	######## YEAR, MONTH, DAY
	$items[$i] =~ s/(\bYEAR\b|\bMONTH\b|\bDAY\b)\((.*?)\)/EXTRACT\(\1 FROM \2\)/igsm;
	######## TO_DAYS()
	$items[$i] =~ s/\bTO_DAYS\((.+?)\)/$1/igsm;
#	$items[$i] =~ s/\bTO_DAYS\((.+?)\)/EXTRACT\(DAY FROM TO_TIMESTAMP\(\1,'YYYY-MM-DD HH24:MI:SS'\) - TO_TIMESTAMP\('0001-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'\) + NUMTODSINTERVAL\( 364 , 'DAY' \)\)/igsm;
	######## DAYOFYEAR()
	$items[$i] =~ s/\bDAYOFYEAR\((.+?)\)/TO_CHAR(\1\,'DDD')/igsm;
	######## LOCATE(), POSITION()
	if ($items[$i] =~ m/(\bLOCATE\((.+?),(.+?)\)|\bPOSITION\((.+?)\s+IN\s+(.+?)\))/igsm) {
		$items[$i] =~ s/'\0'/'00'/;
		$items[$i] =~ s/\bLOCATE\((.+?),(.+?)\)/POSITION\(\1 IN \2\)/igsm;
#		$items[$i] =~ s/\bPOSITION\((.+?)\s+IN\s+(.+?)\)/INSTR\(\2,\1\)/igsm;
	}
	######## IF()
	$items[$i] =~ s/\bIF\((.+?),(.+?),(.+?)\)/(CASE WHEN $1 THEN $2 ELSE $3 END)/igms;
	# $items[$i] =~ s/\bIF\((.+?),(.+?),(.+?)\)/IF $1 THEN $2 ELSE $3 END IF/igsm;

############### В подзапросах и функциях (проще, чем в основном запросе) Вставляем AS перед алиасами (на случай совпадения их с ключевыми словами)
if ($items[$i] =~ m/\bSELECT\b.*\bFROM\b/ims) {
  my $select_from = [];
  my $sfi = 0;

  while ($items[$i] =~ m/\bSELECT\b.*\bFROM\b/igms) {
    my $str;
    if ($items[$i] =~ s/(\bSELECT\b)(.*?)(\bSELECT\b.*?\bFROM\b)/$1$2SlCtFrM$sfi/ims) {

      $str = $3;
    } elsif ($items[$i] =~ s/\bSELECT\b.*\bFROM\b/SlCtFrM$sfi/ims) { # замена последнего оставшегося select...from
      $str = $&;
    }

    my $case_end = [];
    my $csi=0;

    if ($str =~ m/\bCASE\b.*\bEND\b/ims) {
      while ($str =~ s/(\bCASE\b)(.*)(\1.*?\bEND\b)/$1$2CSNDcsnd$csi/ims) {
        $case_end[$csi++] = $3;
      }
      $str =~ s/\bCASE\b.*\bEND\b/CSNDcsnd$csi/ims; # замена последнего оставшегося case...end
      $case_end[$csi] = $&;
    }

    if ($str =~ m/\bSELECT\b\s*(DISTINCT\s*)?(.*)\bFROM\b/igms) {
      my $list_col = $2;
      $list_col =~ s/(\w+)\b(\s+AS\s*)?\s+\b(\w+)/$1 AS $3/igms;
      $str =~ s/(\bSELECT\b\s*(DISTINCT\s*)?)(.*)(\bFROM\b)/$1$list_col$4/ims;
    }

    while ($str =~ s/CSNDcsnd(\d+)/$case_end[$1]/igsm) {}
    $select_from[$sfi++] = $str;
  }

  while ($items[$i] =~ s/SlCtFrM(\d+)/$select_from[$1]/igsm) {}
}
	##############################################################################
	# Заполняем шаблон верхнего уровня ранее запомненными и измененными items
	# в помеченных местах
	##############################################################################
	$sql =~ s/NJNJNjgyyuypoht$i/$items[$i]/gsm;
	# Просматриваем поля и заменяем если в них есть текущий шаблон (для дальнейшей замены GROUP BY 1,2,3 ...)
	if ($need_group_by) {
		for (my $x = 0; $x <= $#group_by_fields_ref; $x++) {
			for (my $y = 0; $y <= $#{@{$group_by_fields_ref[$x]}}; $y++) {
				$group_by_fields_ref [$x] -> [$y] =~ s/NJNJNjgyyuypoht$i/$items[$i]/gsm;
			}
		}
	}

	### Заменяем двойные AS AS на одинарные
	$sql =~ s/\bAS\s+AS\b/AS/gsm;
}


################ Меняем GROUP BY 1,2,3 ...

#if ($need_group_by) {
#	my (@result,$group_by);
#
#	for (my $x = 0; $x <= $#group_by_values_ref; $x++) {
#		for (my $y = 0; $y <= $#{@{group_by_values_ref[$x]}}; $y++) {
#			my $index = $group_by_values_ref [$x] -> [$y];
#			# Если в GROUP BY стояла цифра - заменяем на значение
#			if ($index =~ m/\b\d+\b/igsm) {
#				push @result,$group_by_fields_ref[$x]->[$index-1];
#			}
#			# иначе - то что стояло
#			else {
#				push @result,$group_by_values_ref[$x]->[$y];
#			}
#
#		}
#		# Формируем GROUP BY для каждого SELECT
#		$group_by = join(',',@result);
#		$sql =~ s/VJkjn;lohggff/\n GROUP BY $group_by /sm;
#		@result=();
#	}
#}

############### IF()
$sql =~ s/\bIF\((.+?),(.+?),(.+?)\)/(CASE WHEN $1 THEN $2 ELSE $3 END)/igms;

############### Делаем регистронезависимый LIKE
#$sql =~ s/([\w\'\?\.\%\_]*?\s+)(NOT\s+)*LIKE(\s+[\w\'\?\.\%\_]*?[\s\)]+)/ UPPER\(\1\) \2 LIKE UPPER\(\3\) /igsm;
$sql =~ s/\sLIKE\s/ ILIKE /igsm;

############### Удаляем псевдофункции
$sql =~ s/VGtygvVGVYbbhyh//gsm;
# Восстанавливаем то что было внутри кавычек НЕ в аргументах функций
$sql =~ s/POJJNBhvtgfckjh(\d+)/$in_quotes[$1]/gsm;
# Восстанавливаем скобки и запятые в кавычках
$sql =~ s/JKghsdgfweftyfd/\(/gsm;
$sql =~ s/RTYfghhfFGhhjJg/\)/gsm;
$sql =~ s/DFgpoUUYTJjkgJj/\,/gsm;
# добавляем FROM DUAL если в SELECT не задано FROM
#if ($need_from_dual) {
#	$sql =~ s/\n//igsm;
#	$sql .= " FROM DUAL\n";
#}

################# Эти замены необходимо делать только после всех преобразований
# , потому что сборка идет с верхнего уровня и мы заранее не знаем что будет стоять
# в параметрах этих функций после всех замен
#################
# Делаем из (TO_TIMESTAMP(CURRENT_TIMESTAMP)) просто CURRENT_TIMESTAMP
$sql =~ s/TO_TIMESTAMP\(CURRENT_TIMESTAMP,'YYYY-MM-DD HH24:MI:SS'\)/CURRENT_TIMESTAMP/igsm;
#################
# В случае если у нас данные хранятся в Unicode и есть явно заданные литералы
# внутри CASE ... END - передаем литералы  в UNISTR()
###################################################################################
#if ($model_update -> {characterset} =~ /UTF/i) {

#	my $new_sql;
#	while ($sql =~ m/\bCASE\s+(.*?WHEN\s+.*?THEN\s+.*?ELSE\s+.*?END)/ism) {
#		$new_sql .= $`;
#		$sql = $';
#		my $temp = $1;
#		$temp =~ s/('.*?')/UNISTR\(\1\)/igsm;
#		$new_sql .= " CASE $temp ";
#	}
#	$new_sql .= $sql;
#	$sql = $new_sql;
#}

#warn "ORACLE OUT: <$sql>\n";

# Заменим CAST char->text, signed->int
$sql =~ s/(CAST\(.+?\s*AS\s*)(char)\s*\)/$1text)/igsm;
$sql =~ s/(CAST\(.+?\s*AS\s*)(signed)\s*\)/$1int)/igsm;

############### Восстанавливаем COUNT(*) OVER()
$sql =~ s/CnTOveR/COUNT(*) OVER()/igsm;

############### Убираем FROM DUAL
$sql =~ s/\bFROM\b\s+\bDUAL\b//igsm;

$mysql_to_postgresql_cache -> {$src_sql} = $sql if ($src_sql !~ /\bIF\((.+?),(.+?),(.+?)\)/igsm);

return $sql;

}

################################################################################

sub sql_lock {

	sql_do ("LOCK TABLE $_[0] IN EXCLUSIVE MODE");

}

################################################################################

sub sql_unlock {

	# do nothing, wait for commit/rollback

}

################################################################################

sub _sql_ok_subselects { 1 }

################################################################################

sub get_sql_translator_ref {

	return \ &mysql_to_postgresql if $conf -> {core_auto_postgresql};

}

################################################################################
################################################################################

#package DBIx::ModelUpdate::PostgreSQL;

#use Data::Dumper;

#no warnings;

#our @ISA = qw (DBIx::ModelUpdate);

################################################################################

sub prepare {

	my ($self, $sql) = @_;

	return $self -> {db} -> prepare ($sql);

}

1;
