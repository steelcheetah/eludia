columns => {
	type        => 'string(255)', # ��� �������
	id_object   => 'int',         # ������������� �������
	id_user     => 'int',         # ������������
	params      => 'text',        # �������� get �������, ����� ���������
	dt          => 'timestamp',   # ���� �������
	duration    => 'int',         # �����������������
},

keys => {
	type   => 'type',
}