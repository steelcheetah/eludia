# Тестирование конвертора mysql_to_postgresql{} в PostgreSQL.pm
# Запросы под mysql сначала проходят конвертирование, а потом выполняются в postgresql

# Запуск командой perl -I/var/projects/eludia_housing -MEludia::Install -e "test()" psql_converter

use Test::More tests => 12;

cleanup ();

my @tables = ({
	name    => 'table1',

	columns => {

		id    => {TYPE_NAME => 'int', _EXTRA => 'auto_increment', _PK => 1},
		fake  => {TYPE_NAME => 'bigint'},
		name  => {TYPE_NAME => 'varchar', COLUMN_SIZE => 255},
		label => {TYPE_NAME => 'varchar', COLUMN_SIZE => 255},
		weight=> {TYPE_NAME => 'varchar'},
	},
	data => [
		{id => 1, fake => 0, name => 'one',   label => 'The One',   weight => '4'},
		{id => 2, fake => 0, name => 'two',   label => 'The Two',   weight => '2'},
		{id => 3, fake => 0, name => 'three', label => 'The Three', weight => '1'},
		{id => 4, fake => 0, name => 'four',  label => 'The Four',  weight => '3'},
	]},

	{name    => 'table2',

	columns => {

		id    => {TYPE_NAME => 'int', _EXTRA => 'auto_increment', _PK => 1},
		fake  => {TYPE_NAME => 'bigint'},
		name  => {TYPE_NAME => 'varchar', COLUMN_SIZE => 255},
		label => {TYPE_NAME => 'varchar', COLUMN_SIZE => 255},
		id_log=> {TYPE_NAME => 'bigint'},
	},
	data => [
		{fake => 0, name => 'name1', label => 'The One',   id_log => 4},
		{fake => 0, name => 'name2', label => 'The Two',   id_log => 2},
		{fake => 0, name => 'name3', label => 'The Three', id_log => 1},
		{fake => 0, name => 'name4', label => 'The Four',  id_log => 3},
	]}
);

wish (tables => Storable::dclone \@tables, {});

foreach my $i (@tables) {

	wish (table_columns => [map {{name => $_, %{$i -> {columns} -> {$_}}}}    (keys %{$i -> {columns}})], {table => $i -> {name}});

	wish (table_data => $i -> {data}, {table => $i -> {name}, key   => 'id'});
}


### Test 1
$res = eval {
	sql_do('SELECT IF(name = "one", 1, 0) one FROM table1 WHERE fake = 0')
};
error_output($@, 'Double quote -> single qoute && IF -> CASE & syntax AS && Boolean');

### Test 2
my $res = eval {
	sql_do('SELECT CURRENT_DATE(), IFNULL(NULL, 1)')
};
error_output($@, 'IFNULL -> COALESCE && CURRENT_DATE');

### Test 3
$res = eval {
	sql_do('SELECT name id, label label FROM table1 WHERE fake = 0 ORDER BY NULL LIMIT 50')
};
error_output($@, 'Syntax AS && ORDER BY NULL && LIMIT 50');

### Test 4
$res = eval {
	sql_do('SELECT DATE_SUB(NOW(), INTERVAL 1 MINUTE)')
};
error_output($@, 'DATE_SUB');

### Test 5
$res = eval {
	sql_do('SELECT * from `table1`')
};
error_output($@, 'Backtick');

### Test 6
$res = eval {
	sql_do('SELECT * FROM table1 ORDER BY weight + 0 ASC LIMIT 1, 3')
};
error_output($@, 'LIMIT x,y -> LIMIT x OFFSET y && ORDER BY ... + 0');

### Test 7
$res = eval {
	sql_do("SELECT GROUP_CONCAT(DISTINCT name) all_names FROM table1 WHERE fake = 0")
};
error_output($@, 'AS && GROUP_CONCAT -> ARRAY_AGG');

### Test 8
$res = eval {
	sql_do("INSERT INTO table2(label, name) (SELECT label label, (CASE WHEN 1 = 0 THEN 'five' ELSE 'six' END) name FROM table1 WHERE id = 4 GROUP BY label)")
};
error_output($@, 'AS inside subquery');

### Test 9
$res = eval {
	sql_do("UPDATE table1
                LEFT JOIN table2 ON table1.id = table2.id_log
	        SET
	                table1.name = CONCAT(table1.name, ' -> ', table2.name)
	        WHERE
	                table1.fake=0")
};
error_output($@, 'UPDATE...JOIN');

### Test 10
$res = eval {
	sql_do("SELECT 
				GROUP_CONCAT(IF(IFNULL(NULL, 1) < 5, table1.id, NULL)) all_names 
			FROM table2 
    		LEFT JOIN table1 
    			ON table1.id = table2.id_log
    		WHERE table1.fake = 0")
};
error_output($@, 'ALL: GROUP_CONCAT && IF && IFNULL && AS && JOIN');


### Test 11
my $i = 30;
$res = eval {
	sql_do ("SELECT NOW() - INTERVAL ? MINUTE", $i) # mysql
};
error_output($@, 'INTERVAL ? MINUTE');

### Test 12
$res = eval {
	sql_do ("SELECT * FROM table1 WHERE weight NOT IN ('1', '3')") 
};
error_output($@, 'Явное приведение типов в IN');

################################################################################

sub error_output {
	my ($full_err, $name_test) = @_;
	my ($err) = split /at/, $full_err;
	is ($err, undef, $name_test);
}

sub cleanup {
	foreach my $table (@tables) {
		my $name = $table -> {name};
		eval {sql_do ("DROP TABLE $name")};
	}
}

# END  { cleanup () }
END  {  }
