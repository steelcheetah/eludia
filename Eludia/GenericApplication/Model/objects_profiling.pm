columns => {
	type        => 'string(255)', # Тип объекта
	id_object   => 'int',         # Идентификатор объекта
	id_user     => 'int',         # Пользователь
	params      => 'text',        # Парамеры get запроса, кроме системных
	dt          => 'timestamp',   # Дата запроса
	duration    => 'int',         # Продолжительность
},

keys => {
	type   => 'type',
}